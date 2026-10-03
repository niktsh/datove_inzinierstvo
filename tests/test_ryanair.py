import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from krakow_di.collectors.ryanair import (
    RyanairCollector,
    RyanairConfig,
    load_config,
    parse_fares,
    parse_routes,
)

FIX = Path(__file__).parent / "fixtures" / "ryanair"
NOW = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)


def load(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


ROUTES = load("routes_from_krk.json")
FARES = load("bcn_krk_two_days.json")
EMPTY = load("empty.json")
CANDIDATES = ("BCN", "VIE", "ARN", "OSL", "ZZZ")


def test_load_real_config():
    cfg = load_config("config/routes_ryanair.yaml")
    assert cfg.destination == "KRK" and len(cfg.candidates) >= 10 and cfg.pause_seconds >= 1


def test_parse_routes_intersects_candidates_and_takes_zone():
    zones = parse_routes(ROUTES, CANDIDATES)
    assert zones == {"BCN": "Europe/Madrid", "VIE": "Europe/Vienna", "ARN": "Europe/Stockholm"}
    assert parse_routes([{"junk": 1}, None, {"arrivalAirport": {"code": "BCN"}}], CANDIDATES) == {}


def test_parse_fares_real_fixture():
    (o,) = parse_fares(FARES, "BCN", "Europe/Madrid", "KRK", "Europe/Warsaw", NOW)
    assert o.source == "ryanair" and o.origin_iata == "BCN" and o.destination_iata == "KRK"
    assert o.flight_number == "FR3035" and o.airline_iata == "FR" and o.stops == 0
    assert o.price == Decimal("45.99") and o.currency == "EUR"
    # naive local times get the airport offsets (CEST, UTC+2, on 21 Oct 2026)
    assert o.departure_at.isoformat() == "2026-10-21T18:35:00+02:00"
    assert o.arrival_at.isoformat() == "2026-10-21T21:25:00+02:00"
    assert o.departure_at.astimezone(UTC).hour == 16


def test_dst_offset_differs_in_winter():
    body = json.loads(json.dumps(FARES))
    ob = body["fares"][0]["outbound"]
    ob["departureDate"], ob["arrivalDate"] = "2026-11-10T11:05:00", "2026-11-10T14:00:00"
    (o,) = parse_fares(body, "BCN", "Europe/Madrid", "KRK", "Europe/Warsaw", NOW)
    assert o.departure_at.isoformat() == "2026-11-10T11:05:00+01:00"


def test_empty_malformed_past_foreign():
    def p(body):
        return parse_fares(body, "BCN", "Europe/Madrid", "KRK", "Europe/Warsaw", NOW)

    assert p(EMPTY) == [] and p({}) == [] and p({"fares": None}) == []
    assert p({"fares": [{"outbound": {}}, "x"]}) == []
    assert parse_fares(FARES, "VIE", "Europe/Vienna", "KRK", "Europe/Warsaw", NOW) == []
    late = datetime(2026, 10, 22, tzinfo=UTC)
    assert parse_fares(FARES, "BCN", "Europe/Madrid", "KRK", "Europe/Warsaw", late) == []


CFG = RyanairConfig("KRK", "Europe/Warsaw", 3, "EUR", "en-gb", 1.5, 3, ("BCN", "VIE", "OSL"))


def fare_for(day: str, origin: str, price=45.99):
    body = json.loads(json.dumps(FARES))
    ob = body["fares"][0]["outbound"]
    ob["departureAirport"]["iataCode"] = origin
    ob["departureDate"], ob["arrivalDate"] = f"{day}T18:35:00", f"{day}T21:30:00"
    ob["price"]["value"] = price
    return body


def make(handler, cfg=CFG, **kw):
    client = httpx.Client(transport=httpx.MockTransport(handler))
    sleeps: list[float] = []
    c = RyanairCollector(cfg, client=client, sleep=sleeps.append, now=lambda: NOW, **kw)
    return c, sleeps


def ok_handler(req):
    if "routes" in req.url.path:
        return httpx.Response(200, json=ROUTES)
    p = req.url.params
    assert p["outboundDepartureDateFrom"] == p["outboundDepartureDateTo"]
    day, origin = p["outboundDepartureDateFrom"], p["departureAirportIataCode"]
    return httpx.Response(200, json=fare_for(day, origin))


def test_run_one_request_per_origin_and_day(conn):
    seen = []

    def handler(req):
        seen.append(req)
        return ok_handler(req)

    c, sleeps = make(handler)
    s = c.run(conn)
    fares = [r for r in seen if "oneWayFares" in r.url.path]
    # BCN and VIE are active (OSL is not a Ryanair route to KRK), 3 days each
    assert len(fares) == 6 and s.requests == 6 and s.errors == 0 and not s.aborted
    assert {(r.url.params["departureAirportIataCode"]) for r in fares} == {"BCN", "VIE"}
    assert [r.url.params["outboundDepartureDateFrom"] for r in fares[:3]] == [
        "2026-10-03", "2026-10-04", "2026-10-05"]
    assert sleeps.count(1.5) == 5  # pause between all 6 fare requests
    assert s.counts() == {"found": 6}
    n = conn.execute("SELECT count(*) AS n FROM core.flight_offer WHERE source='ryanair'")
    assert n.fetchone()["n"] == 6
    statuses = conn.execute(
        "SELECT parse_status, count(*) AS n FROM raw.fetch_log GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert statuses == [{"parse_status": "ok", "n": 7}]  # 6 fares + route list


def test_second_run_observed_then_price_change(conn):
    state = {"price": 45.99}

    def handler(req):
        if "routes" in req.url.path:
            return httpx.Response(200, json=ROUTES)
        p = req.url.params
        return httpx.Response(
            200, json=fare_for(p["outboundDepartureDateFrom"], "BCN", state["price"])
        )

    c, _ = make(handler)
    c.run(conn, origins=("BCN",), horizon_days=2)
    assert c.run(conn, origins=("BCN",), horizon_days=2).counts() == {"observed": 2}
    state["price"] = 51.5
    s = c.run(conn, origins=("BCN",), horizon_days=2)
    assert s.counts() == {"price_changed": 2}
    assert all(ch.result.old_price == Decimal("45.99") for ch in s.changes)


def test_empty_day_is_not_an_error(conn):
    def handler(req):
        if "routes" in req.url.path:
            return httpx.Response(200, json=ROUTES)
        return httpx.Response(200, json=EMPTY)

    c, _ = make(handler)
    s = c.run(conn, origins=("BCN",))
    assert s.empty == 3 and s.errors == 0 and s.changes == []


def test_block_aborts_run_after_consecutive_errors(conn):
    calls = {"fares": 0}

    def handler(req):
        if "routes" in req.url.path:
            return httpx.Response(200, json=ROUTES)
        calls["fares"] += 1
        return httpx.Response(409, json={"message": "Availability declined"})

    cfg = RyanairConfig("KRK", "Europe/Warsaw", 90, "EUR", "en-gb", 1.5, 3, ("BCN", "VIE"))
    c, _ = make(handler, cfg=cfg)
    s = c.run(conn)
    assert s.aborted and s.errors == 3 and calls["fares"] == 3  # 409 is not retried
    rows = conn.execute("SELECT status_code FROM raw.fetch_log WHERE parse_status='error'")
    assert [r["status_code"] for r in rows.fetchall()] == [409, 409, 409]


def test_isolated_errors_do_not_abort(conn):
    n = {"i": 0}

    def handler(req):
        if "routes" in req.url.path:
            return httpx.Response(200, json=ROUTES)
        n["i"] += 1
        if n["i"] == 2:
            return httpx.Response(200, content=b"<html>nope</html>")
        p = req.url.params
        return httpx.Response(200, json=fare_for(p["outboundDepartureDateFrom"], "BCN"))

    c, _ = make(handler)
    s = c.run(conn, origins=("BCN",))
    assert s.errors == 1 and not s.aborted and len(s.changes) == 2


def test_route_list_failure_raises(conn):
    c, _ = make(lambda req: httpx.Response(200, json={"not": "a list"}))
    with pytest.raises(RuntimeError):
        c.run(conn)
    row = conn.execute("SELECT parse_status FROM raw.fetch_log").fetchone()
    assert row["parse_status"] == "error"
