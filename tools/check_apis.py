#!/usr/bin/env python3
"""
Smoke test + coverage report for the project's data sources (Europe only).

Setup:
    pip install httpx python-dotenv
    .env next to this file (repo root):
        TRAVELPAYOUTS_TOKEN=...
        TICKETMASTER_KEY=...
        AVIATIONSTACK_KEY=...      # optional
    uv run python tools/check_apis.py

Output: console tables + ./samples/ (raw responses, summary.json, coverage.json)
"""

import json
import os
import socket
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

SAMPLES = Path("samples")
SAMPLES.mkdir(exist_ok=True)
results = []  # {"check", "ok", "detail"}

# name, city IATA (Travelpayouts), Ticketmaster country code, Ticketmaster city spellings
CITIES = [
    ("Bratislava", "BTS", "SK", ["Bratislava"]),
    ("Košice", "KSC", "SK", ["Košice", "Kosice"]),
    ("Vienna", "VIE", "AT", ["Vienna", "Wien"]),
    ("Prague", "PRG", "CZ", ["Prague", "Praha"]),
    ("Budapest", "BUD", "HU", ["Budapest"]),
    ("Warsaw", "WAW", "PL", ["Warsaw", "Warszawa"]),
    ("Krakow", "KRK", "PL", ["Krakow", "Kraków"]),
    ("Berlin", "BER", "DE", ["Berlin"]),
    ("Munich", "MUC", "DE", ["Munich", "München"]),
    ("Paris", "PAR", "FR", ["Paris"]),
    ("London", "LON", "GB", ["London"]),
    ("Amsterdam", "AMS", "NL", ["Amsterdam"]),
    ("Brussels", "BRU", "BE", ["Brussels", "Bruxelles"]),
    ("Madrid", "MAD", "ES", ["Madrid"]),
    ("Barcelona", "BCN", "ES", ["Barcelona"]),
    ("Rome", "ROM", "IT", ["Rome", "Roma"]),
    ("Milan", "MIL", "IT", ["Milan", "Milano"]),
    ("Copenhagen", "CPH", "DK", ["Copenhagen", "København"]),
    ("Stockholm", "STO", "SE", ["Stockholm"]),
    ("Dublin", "DUB", "IE", ["Dublin"]),
    ("Zurich", "ZRH", "CH", ["Zurich", "Zürich"]),
]
ORIGINS = ["BTS", "VIE"]  # departure cities to test flight coverage from


def record(name, ok, detail=""):
    results.append({"check": name, "ok": ok, "detail": detail})
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}  {detail}")


def save(name, data):
    (SAMPLES / f"{name}.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2)
    )


def rate_headers(resp):
    return {
        k: v
        for k, v in resp.headers.items()
        if "rate" in k.lower() or "quota" in k.lower()
    }


def get(url, **kw):
    """GET with one retry on HTTP 429. Returns Response or Exception."""
    for attempt in range(2):
        try:
            r = httpx.get(url, timeout=20, **kw)
        except Exception as e:
            return e
        if r.status_code == 429 and attempt == 0:
            time.sleep(2)
            continue
        return r


# ---------------------------------------------------------------- 1. domains
DOMAINS = [
    ("api.travelpayouts.com", "https"),
    ("app.ticketmaster.com", "https"),
    ("api.aviationstack.com", "http"),  # free plan = HTTP only
    ("pypi.org", "https"),
    ("registry-1.docker.io", "https"),
]


def check_domains():
    print("\n1) Domain reachability (404/403 on the root path is normal for API hosts)")
    for host, scheme in DOMAINS:
        try:
            ip = socket.gethostbyname(host)
        except OSError as e:
            record(f"DNS {host}", False, str(e))
            continue
        r = get(f"{scheme}://{host}", follow_redirects=True)
        if isinstance(r, Exception):
            record(
                f"{scheme.upper()} {host}", False, f"{ip} -> {type(r).__name__}: {r}"
            )
        else:
            record(f"{scheme.upper()} {host}", True, f"{ip} -> HTTP {r.status_code}")


# ------------------------------------------------------------ 2. Travelpayouts
TP_URL = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
TP_EXPECTED = [
    "origin",
    "destination",
    "price",
    "airline",
    "flight_number",
    "departure_at",
    "transfers",
    "duration",
]


def tp_query(token, origin, dest, month):
    return get(
        TP_URL,
        params={
            "origin": origin,
            "destination": dest,
            "departure_at": month,
            "one_way": "true",
            "currency": "eur",
            "sorting": "price",
            "limit": 30,
        },
        headers={"X-Access-Token": token, "Accept-Encoding": "gzip"},
    )


def check_travelpayouts():
    print("\n2) Travelpayouts Data API (flights)")
    token = os.getenv("TRAVELPAYOUTS_TOKEN")
    coverage = {}
    if not token:
        record("Travelpayouts token", False, "TRAVELPAYOUTS_TOKEN not set")
        return coverage
    month = (date.today() + timedelta(days=30)).strftime("%Y-%m")
    auth_ok, sample_saved = False, False
    for name, iata, _, _ in CITIES:
        coverage[name] = {}
        for origin in ORIGINS:
            if origin == iata:
                coverage[name][origin] = None
                continue
            r = tp_query(token, origin, iata, month)
            time.sleep(0.2)
            if isinstance(r, Exception) or r.status_code != 200:
                coverage[name][origin] = "err"
                continue
            body = r.json()
            data = body.get("data") or []
            auth_ok = auth_ok or body.get("success") is True
            coverage[name][origin] = {
                "offers": len(data),
                "min_price": min((d["price"] for d in data), default=None),
            }
            if data and not sample_saved:
                save("travelpayouts_prices_for_dates", body)
                missing = [f for f in TP_EXPECTED if f not in data[0]]
                record(
                    "Travelpayouts expected fields",
                    not missing,
                    f"missing: {missing}" if missing else "all present",
                )
                hdrs = rate_headers(r)
                if hdrs:
                    print("     rate-limit headers:", hdrs)
                sample_saved = True
    record(
        "Travelpayouts auth + responses", auth_ok, f"month {month}, origins {ORIGINS}"
    )
    print(
        "     NOTE: cached prices (cheapest found by users in the last 48h), no availability flag."
    )
    return coverage


# ------------------------------------------------------------- 3. Ticketmaster
TM_URL = "https://app.ticketmaster.com/discovery/v2/events.json"


def tm_city(key, country, variants):
    """Try city spellings until one returns events. Returns (spelling, total, events) or None."""
    for spelling in variants:
        r = get(
            TM_URL,
            params={
                "apikey": key,
                "city": spelling,
                "countryCode": country,
                "classificationName": "theatre",
                "size": 200,
                "sort": "date,asc",
            },
        )
        time.sleep(0.3)  # stay under 5 req/s
        if isinstance(r, Exception) or r.status_code != 200:
            code = (
                "network error" if isinstance(r, Exception) else f"HTTP {r.status_code}"
            )
            print(f"     ! {spelling}: {code}")
            continue
        body = r.json()
        total = body.get("page", {}).get("totalElements", 0)
        if total:
            return (
                spelling,
                total,
                body.get("_embedded", {}).get("events", []),
                body,
                rate_headers(r),
            )
    return None


def check_ticketmaster():
    print("\n3) Ticketmaster Discovery API (theatre events)")
    key = os.getenv("TICKETMASTER_KEY")
    coverage = {}
    if not key:
        record("Ticketmaster key", False, "TICKETMASTER_KEY not set")
        return coverage
    all_status, saved = Counter(), False
    for name, _, country, variants in CITIES:
        found = tm_city(key, country, variants)
        if not found:
            coverage[name] = {"total": 0}
            continue
        spelling, total, events, body, hdrs = found
        with_price = sum(1 for e in events if e.get("priceRanges"))
        statuses = Counter(
            e.get("dates", {}).get("status", {}).get("code", "?") for e in events
        )
        all_status.update(statuses)
        coverage[name] = {
            "total": total,
            "sampled": len(events),
            "with_price": with_price,
            "spelling": spelling,
            "statuses": dict(statuses),
        }
        if not saved:
            save("ticketmaster_events_sample", body)
            print("     rate-limit headers:", hdrs)
            saved = True
    good = [n for n, c in coverage.items() if c["total"] >= 20]
    record(
        "Ticketmaster auth + coverage",
        bool(good),
        f"{len(good)} cities with >=20 theatre events",
    )
    print("     status codes seen in sampled events:", dict(all_status))
    return coverage


# ------------------------------------------------------------- 4. AviationStack
def check_aviationstack():
    print("\n4) AviationStack (optional, flight status - not prices)")
    key = os.getenv("AVIATIONSTACK_KEY")
    if not key:
        print("  skipped (AVIATIONSTACK_KEY not set)")
        return
    r = get(
        "http://api.aviationstack.com/v1/flights",
        params={"access_key": key, "limit": 1},
    )
    if isinstance(r, Exception):
        record("AviationStack request", False, str(r))
        return
    body = r.json()
    save("aviationstack_flights", body)
    record(
        "AviationStack response",
        r.status_code == 200 and "data" in body,
        f"HTTP {r.status_code}"
        + (f", error={body['error']}" if "error" in body else ""),
    )


# ------------------------------------------------------------- 5. report
def print_report(tp, tm):
    print(
        "\n5) Coverage report (theatres = Ticketmaster, flights = Travelpayouts, next month)"
    )
    head = f"{'City':<12}{'TM events':>10}{'with price':>12}   " + "".join(
        f"{'from ' + o:>16}" for o in ORIGINS
    )
    print(head)
    print("-" * len(head))
    for name, *_ in CITIES:
        c = tm.get(name)
        if c is None:
            ev, pr = "?", "?"
        else:
            ev = str(c["total"])
            pr = f"{c['with_price']}/{c['sampled']}" if c["total"] else "-"
        cells = []
        for o in ORIGINS:
            t = tp.get(name, {}).get(o, "?")
            if t is None:
                cells.append("-")
            elif t == "err" or t == "?":
                cells.append("err/?")
            else:
                cells.append(
                    f"{t['offers']} (min {t['min_price']}€)" if t["offers"] else "0"
                )
        print(f"{name:<12}{ev:>10}{pr:>12}   " + "".join(f"{x:>16}" for x in cells))
    both = [
        n
        for n, *_ in CITIES
        if (tm.get(n) or {}).get("total", 0) >= 20
        and any(isinstance(v, dict) and v["offers"] for v in tp.get(n, {}).values())
    ]
    print(
        "\nCandidate destinations (>=20 theatre events AND flight offers found):",
        ", ".join(both) or "none",
    )
    save(
        "coverage",
        {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "theatres": tm,
            "flights": tp,
            "candidates": both,
        },
    )


if __name__ == "__main__":
    print(f"API smoke test, {datetime.now():%Y-%m-%d %H:%M}")
    check_domains()
    tp_cov = check_travelpayouts()
    tm_cov = check_ticketmaster()
    check_aviationstack()
    if tp_cov or tm_cov:
        print_report(tp_cov, tm_cov)

    failed = [r for r in results if not r["ok"]]
    print(f"\nSummary: {len(results) - len(failed)} passed, {len(failed)} failed")
    for r in failed:
        print(f"  - {r['check']}: {r['detail']}")
    save(
        "summary",
        {"run_at": datetime.now().isoformat(timespec="seconds"), "results": results},
    )
    sys.exit(1 if failed else 0)
