import asyncio
import json
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlparse

import pytest
from fastapi.testclient import TestClient
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from starlette.websockets import WebSocketDisconnect

from krakow_di.api.app import create_app
from krakow_di.api.feed import event_feed
from krakow_di.api.filters import InvalidFilter, matches, parse_types, to_like
from krakow_di.api.hub import EventHub
from krakow_di.config import Settings
from krakow_di.events.envelope import make_event
from krakow_di.events.publisher import store_events
from krakow_di.generator.engine import Generator
from krakow_di.generator.model import ModelParams
from krakow_di.repo import theater as trepo
from krakow_di.repo.flight_offers import OfferObservation, upsert_offer
from krakow_di.repo.theater import CategorySeats, Performance, Sale

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)


def settings_for(url: str) -> Settings:
    u = urlparse(url)
    return Settings(_env_file=None, postgres_host=u.hostname, postgres_port=u.port,
                    postgres_user=u.username, postgres_password=u.password,
                    postgres_db=u.path.lstrip("/"))


def sold_out(offer_id: str, at: datetime) -> dict:
    return make_event("flight.offer.sold_out", "generator",
                      {"offer_id": offer_id, "last_price": 10.0, "currency": "EUR"}, at, "p")


def seed(conn):
    for i in range(6):
        upsert_offer(conn, OfferObservation(
            source="ryanair" if i % 2 else "travelpayouts",
            origin_iata="BCN" if i < 3 else "VIE", destination_iata="KRK",
            departure_at=NOW + timedelta(days=3 + i), airline_iata="FR",
            price=Decimal(40 + i), currency="EUR", observed_at=NOW, flight_number=f"FR{i}"))
    conn.commit()
    g = Generator(ModelParams(base_rate=0.3), 5, "p")
    g.tick(conn, NOW, 0)
    for k in range(1, 40):
        g.tick(conn, NOW + timedelta(minutes=5 * k), 300)
    perf = Performance("wielki-gatsby-2026-11-03-19-00", "Wielki Gatsby", "Scena MOS",
                       datetime(2026, 11, 3, 19, tzinfo=UTC), "https://x/kup-bilet/g", "on_sale", 1)
    trepo.upsert_performance(conn, perf, NOW)
    trepo.insert_snapshot(conn, perf.performance_id, NOW,
                          [CategorySeats("Bilet", Decimal("100.00"), 80)], "PLN",
                          Decimal("4.3775"), {("Bilet", Decimal("100.00")): Decimal("22.84")})
    trepo.insert_sale(conn, Sale(
        __import__("uuid").uuid4(), perf.performance_id, "Bilet", 2, Decimal("100.00"), "PLN",
        Decimal("22.84"), Decimal("4.3775"), NOW, NOW + timedelta(hours=3)))
    store_events(conn, [sold_out(f"{i:016x}", NOW + timedelta(seconds=i)) for i in range(5)]
                 + [make_event("theater.tickets.sold", "theater", {
                     "performance_id": perf.performance_id, "category": "Bilet", "quantity": 2,
                     "unit_price": 100.0, "currency": "PLN", "unit_price_eur": 22.84,
                     "fx_rate": 4.3775, "detected_between": ["2026-10-10T12:00:00Z",
                                                              "2026-10-10T15:00:00Z"]},
                                  NOW, "p")], "krakow")
    conn.commit()


@pytest.fixture
def api(conn, migrated_db):
    seed(conn)
    app = create_app(settings_for(migrated_db), start_gateway=False)
    with TestClient(app) as client:
        client.app_state = app.state
        yield client


# ---------------------------------------------------------------- filtre
def test_filters():
    assert parse_types(None) == [] and parse_types("") == []
    assert parse_types("flight.ticket.sold, theater.*") == ["flight.ticket.sold", "theater.*"]
    for bad in ("DROP TABLE", "a;b", "x" * 80, ",".join(["a"] * 21), "flight/..", "a%"):
        with pytest.raises(InvalidFilter):
            parse_types(bad)
    assert matches("theater.tickets.sold", ["theater.*"]) and matches("x", [])
    assert not matches("flight.ticket.sold", ["theater.*"])
    assert to_like(["flight.ticket.*", "a_b"]) == ["flight.ticket.%", "a\\_b"]


def test_hub_drops_slow_subscriber():
    async def run():
        hub = EventHub(queue_size=2)
        sub = hub.subscribe()
        for i in range(3):
            hub.publish((i, {"event_type": "x"}))
        return sub.overflowed, hub.subscribers

    assert asyncio.run(run()) == (True, 0)


# ---------------------------------------------------------------- REST
def test_health_stats_and_openapi(api):
    h = api.get("/health").json()
    assert h["status"] == "ok" and h["db"] is True and h["kafka_gateway"] is False
    assert h["last_event_seq"] >= 6
    s = api.get("/api/v1/stats").json()
    assert s["offers"] == 6 and s["ticket_sales"] > 0 and s["theater_sales"] == 1
    assert s["events"] == 6 and s["seats_sold"] >= s["ticket_sales"]
    paths = api.get("/openapi.json").json()["paths"]
    for p in ("/api/v1/offers", "/api/v1/sales", "/api/v1/events", "/stream",
              "/api/v1/theater/performances"):
        assert p in paths
    assert api.get("/").json()["docs"] == "/docs"
    assert api.options("/api/v1/offers", headers={
        "Origin": "https://x", "Access-Control-Request-Method": "GET"}
    ).headers["access-control-allow-origin"] == "*"


def test_offers_filters_pagination_and_detail(api):
    body = api.get("/api/v1/offers").json()
    assert body["total"] == 6 and len(body["items"]) == 6
    dates = [o["departure_at"] for o in body["items"]]
    assert dates == sorted(dates)
    assert api.get("/api/v1/offers?origin=bcn").json()["total"] == 3
    assert api.get("/api/v1/offers?source=ryanair").json()["total"] == 3
    assert api.get("/api/v1/offers?status=expired").json()["total"] == 0
    page = api.get("/api/v1/offers?limit=2&offset=4").json()
    assert page["total"] == 6 and len(page["items"]) == 2 and page["offset"] == 4
    frm = (NOW + timedelta(days=6)).isoformat()
    assert api.get("/api/v1/offers", params={"departure_from": frm}).json()["total"] == 3
    first = body["items"][0]
    detail = api.get(f"/api/v1/offers/{first['offer_id']}").json()
    assert detail["offer_id"] == first["offer_id"] and detail["seats_total"] > 0
    assert api.get("/api/v1/offers/neexistuje").status_code == 404
    assert api.get("/api/v1/offers?limit=0").status_code == 422
    assert api.get("/api/v1/offers?limit=501").status_code == 422
    assert api.get("/api/v1/offers?origin=BARCELONA").status_code == 422


def test_history_and_sales(api):
    oid = api.get("/api/v1/offers").json()["items"][0]["offer_id"]
    h = api.get(f"/api/v1/offers/{oid}/history").json()
    assert h["total"] >= 2 and {r["cause"] for r in h["items"]} >= {"scrape", "generator"}
    times = [r["observed_at"] for r in h["items"]]
    assert times == sorted(times)
    assert api.get("/api/v1/offers/neexistuje/history").status_code == 404
    sales = api.get("/api/v1/sales?limit=500").json()
    assert sales["total"] == api.get("/api/v1/stats").json()["ticket_sales"]
    assert all(s["total_price"] == pytest.approx(s["unit_price"] * s["quantity"], abs=0.01)
               for s in sales["items"])
    first_offer = sales["items"][0]["offer_id"]
    by_offer = api.get("/api/v1/sales", params={"offer_id": first_offer}).json()
    assert by_offer["total"] >= 1 and {s["offer_id"] for s in by_offer["items"]} == {first_offer}
    assert api.get("/api/v1/sales", params={"since": "2999-01-01T00:00:00Z"}).json()["total"] == 0


def test_theater_endpoints(api):
    pid = "wielki-gatsby-2026-11-03-19-00"
    perfs = api.get("/api/v1/theater/performances?status=on_sale").json()
    assert perfs["total"] == 1 and perfs["items"][0]["performance_id"] == pid
    assert api.get("/api/v1/theater/performances?status=past").json()["total"] == 0
    assert api.get(f"/api/v1/theater/performances/{pid}").json()["title"] == "Wielki Gatsby"
    snap = api.get(f"/api/v1/theater/performances/{pid}/snapshots").json()
    assert snap["items"][0]["price_eur"] == 22.84 and snap["items"][0]["currency"] == "PLN"
    sales = api.get("/api/v1/theater/sales", params={"performance_id": pid}).json()
    assert sales["total"] == 1 and sales["items"][0]["quantity"] == 2
    assert api.get("/api/v1/theater/performances/nic").status_code == 404
    assert api.get("/api/v1/theater/performances/nic/snapshots").status_code == 404


def test_event_log_cursor_and_type_filter(api):
    seen, after = [], 0
    while True:
        page = api.get("/api/v1/events", params={"after_seq": after, "limit": 4}).json()
        seen += page["items"]
        if page["next_after_seq"] is None:
            break
        after = page["next_after_seq"]
    assert [e["seq"] for e in seen] == sorted(e["seq"] for e in seen) and len(seen) == 6
    assert all(e["event"]["event_id"] and e["topic"].startswith("krakow.") for e in seen)
    th = api.get("/api/v1/events?types=theater.*").json()["items"]
    assert [e["event"]["event_type"] for e in th] == ["theater.tickets.sold"]
    both = api.get("/api/v1/events?types=theater.*,flight.offer.sold_out").json()["items"]
    assert len(both) == 6
    assert api.get("/api/v1/events?types=a;b").status_code == 422
    assert api.get("/api/v1/events?limit=1001").status_code == 422


# ---------------------------------------------------------------- SSE
def parse_sse(lines) -> list[dict]:
    frames, cur = [], {}
    for line in lines:
        if line == "":
            if cur:
                frames.append(cur)
            cur = {}
        elif not line.startswith(":"):
            key, _, value = line.partition(": ")
            cur[key] = value
    return frames


def read_stream(api, url, **kw):
    with api.stream("GET", url, **kw) as r:
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/event-stream")
        return parse_sse(list(r.iter_lines()))


def test_sse_catch_up_with_header_query_filter_and_max(api):
    seqs = [e["seq"] for e in api.get("/api/v1/events").json()["items"]]
    frames = [f for f in read_stream(api, "/stream?max=3", headers={"Last-Event-ID": "0"})
              if "id" in f]
    assert [int(f["id"]) for f in frames] == seqs[:3]
    f = frames[0]
    event = json.loads(f["data"])
    assert f["event"] == event["event_type"] == "flight.offer.sold_out"
    after = read_stream(api, f"/stream?last_event_id={seqs[2]}&max=3")
    assert [int(x["id"]) for x in after if "id" in x] == seqs[3:6]
    only = [x for x in read_stream(api, "/stream?types=theater.*&last_event_id=0&max=1")
            if "id" in x]
    assert len(only) == 1 and only[0]["event"] == "theater.tickets.sold"
    assert api.get("/stream", headers={"Last-Event-ID": "abc"}).status_code == 422
    assert api.get("/stream?types=a;b").status_code == 422


def test_sse_live_events_arrive_after_connect(api, conn):
    state = api.app_state
    created = {}

    def publish_later():
        time.sleep(0.7)
        ev = sold_out("00000000000000ff", NOW + timedelta(minutes=1))
        store_events(conn, [ev], "krakow")
        seq = conn.execute("SELECT seq FROM core.event_log WHERE event_id=%s",
                           (ev["event_id"],)).fetchone()["seq"]
        created["seq"], created["id"] = seq, ev["event_id"]
        state.hub.publish_threadsafe((seq, ev))

    t = threading.Thread(target=publish_later)
    t.start()
    frames = [f for f in read_stream(api, "/stream?max=1") if "id" in f]
    t.join()
    assert int(frames[0]["id"]) == created["seq"]
    assert json.loads(frames[0]["data"])["event_id"] == created["id"]


def test_feed_has_no_gaps_or_duplicates_between_catch_up_and_live(migrated_db, conn):
    seed(conn)
    last = conn.execute("SELECT max(seq) AS s FROM core.event_log").fetchone()["s"]

    async def run(patterns):
        pool = AsyncConnectionPool(settings_for(migrated_db).database_url, min_size=1,
                                   open=False, kwargs={"row_factory": dict_row})
        await pool.open()
        hub = EventHub()
        sub = hub.subscribe()
        # živé udalosti prišli počas dobiehania: duplicita už vydanej (last-1) a dve nové
        hub.publish((last - 1, {"event_type": "flight.offer.sold_out", "event_id": "dup"}))
        hub.publish((last + 1, {"event_type": "flight.offer.sold_out", "event_id": "new"}))
        hub.publish((last + 2, {"event_type": "theater.tickets.sold", "event_id": "n2"}))
        feed = event_feed(pool, hub, patterns, last - 2, sub)
        out = []
        try:
            while True:
                out.append(await asyncio.wait_for(feed.__anext__(), 1))
        except TimeoutError:
            pass
        await pool.close()
        return [(s, e["event_id"]) for s, e in out]

    everything = asyncio.run(run([]))
    assert [s for s, _ in everything] == [last - 1, last, last + 1, last + 2]  # bez medzier
    ids = [i for _, i in everything]
    assert "dup" not in ids and ids[2:] == ["new", "n2"]  # duplicita zo živého prúdu vypadla
    flights_only = asyncio.run(run(["flight.*"]))
    assert [s for s, _ in flights_only] == [last - 1, last + 1]  # filter platí aj pre živé


# ---------------------------------------------------------------- WebSocket
def test_websocket_catch_up_filter_and_invalid_filter(api):
    with api.websocket_connect("/ws?last_event_id=0&max=2") as ws:
        a, b = ws.receive_json(), ws.receive_json()
    assert a["seq"] < b["seq"] and a["event"]["event_type"] == "flight.offer.sold_out"
    with api.websocket_connect("/ws?types=theater.*&last_event_id=0&max=1") as ws:
        msg = ws.receive_json()
    assert msg["event"]["event_type"] == "theater.tickets.sold" and msg["seq"] >= 1
    with pytest.raises(WebSocketDisconnect) as exc:
        with api.websocket_connect("/ws?types=a;b"):
            pass
    assert exc.value.code == 1008


def test_websocket_live_event(api, conn):
    state = api.app_state

    def publish_later():
        time.sleep(0.7)
        ev = sold_out("00000000000000ee", NOW + timedelta(minutes=2))
        store_events(conn, [ev], "krakow")
        seq = conn.execute("SELECT seq FROM core.event_log WHERE event_id=%s",
                           (ev["event_id"],)).fetchone()["seq"]
        state.hub.publish_threadsafe((seq, ev))

    t = threading.Thread(target=publish_later)
    t.start()
    with api.websocket_connect("/ws?max=1") as ws:
        msg = ws.receive_json()
    t.join()
    assert msg["event"]["data"]["offer_id"] == "00000000000000ee"
