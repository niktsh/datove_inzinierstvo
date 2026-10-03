import json
from datetime import UTC, datetime
from pathlib import Path

import httpx

from krakow_di.collectors.travelpayouts import TravelpayoutsCollector
from krakow_di.config import Settings
from krakow_di.routes import RoutesConfig

FIX = Path(__file__).parent / "fixtures" / "travelpayouts"
NOW = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
REAL = json.loads((FIX / "bcn_2026-10.json").read_text(encoding="utf-8"))
EMPTY = json.loads((FIX / "empty.json").read_text(encoding="utf-8"))


def make(handler, origins=("BCN",), days=20, **kw):
    settings = Settings(_env_file=None, travelpayouts_token="secret-token")
    routes = RoutesConfig("KRK", days, "eur", 0, origins)
    client = httpx.Client(transport=httpx.MockTransport(handler))
    sleeps: list[float] = []
    c = TravelpayoutsCollector(settings, routes, client=client, sleep=sleeps.append,
                               now=lambda: NOW, **kw)
    return c, sleeps


def test_run_fills_db_and_logs_raw(conn):
    seen = []

    def handler(req):
        seen.append(req)
        month = req.url.params["departure_at"]
        return httpx.Response(200, json=REAL if month == "2026-10" else EMPTY)

    c, _ = make(handler, days=40)
    s = c.run(conn)
    assert [r.url.params["departure_at"] for r in seen] == ["2026-10", "2026-11"]
    assert all(r.url.params["destination"] == "KRK" for r in seen)
    assert s.requests == 2 and s.empty == 1 and s.errors == 0
    assert s.counts() == {"found": len(s.changes)} and len(s.changes) > 0
    n = conn.execute("SELECT count(*) AS n FROM core.flight_offer").fetchone()["n"]
    assert n == len(s.changes)
    rows = conn.execute(
        "SELECT parse_status, request_params FROM raw.fetch_log ORDER BY id"
    ).fetchall()
    assert [r["parse_status"] for r in rows] == ["ok", "empty"]
    assert "secret-token" not in json.dumps([r["request_params"] for r in rows])

    again = c.run(conn)  # second scrape: same offers, no price changes
    assert again.counts() == {"observed": len(s.changes)}
    assert conn.execute("SELECT count(*) AS n FROM core.flight_offer").fetchone()["n"] == n


def test_retries_on_429_and_5xx_with_backoff(conn):
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        if calls["n"] == 2:
            return httpx.Response(503)
        return httpx.Response(200, json=EMPTY)

    c, sleeps = make(handler, days=1)  # one month
    s = c.run(conn)
    assert calls["n"] == 3 and s.errors == 0 and s.empty == 1
    assert sleeps == [7.0, 2.0]


def test_persistent_failure_is_logged_and_run_continues(conn):
    def handler(req):
        if req.url.params["origin"] == "BCN":
            return httpx.Response(500)
        return httpx.Response(200, json=REAL)

    c, _ = make(handler, origins=("BCN", "VIE"), days=1, max_attempts=2)
    s = c.run(conn)
    assert s.errors == 1 and len(s.changes) > 0
    statuses = conn.execute(
        "SELECT status_code, parse_status FROM raw.fetch_log ORDER BY id"
    ).fetchall()
    got = [(r["status_code"], r["parse_status"]) for r in statuses]
    assert got == [(500, "error"), (200, "ok")]


def test_network_error_and_invalid_json(conn):
    def handler(req):
        if req.url.params["origin"] == "BCN":
            raise httpx.ConnectError("boom")
        return httpx.Response(200, content=b"<html>not json</html>")

    c, _ = make(handler, origins=("BCN", "VIE"), days=1, max_attempts=2)
    s = c.run(conn)
    assert s.errors == 2 and s.changes == []


def test_price_change_detected_between_runs(conn):
    changed = json.loads(json.dumps(REAL))
    for d in changed["data"]:
        d["price"] = d["price"] + 10
    bodies = iter([REAL, changed])
    c, _ = make(lambda req: httpx.Response(200, json=next(bodies)), days=1)
    c.run(conn)
    s = c.run(conn)
    assert set(s.counts()) == {"price_changed"}
    assert all(ch.result.old_price is not None for ch in s.changes)


def test_requires_token():
    import pytest

    with pytest.raises(RuntimeError):
        TravelpayoutsCollector(Settings(_env_file=None, travelpayouts_token=""),
                               RoutesConfig("KRK", 1, "eur", 0, ("BCN",)))
