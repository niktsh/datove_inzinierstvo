"""Adaptér tímu 4 na zaznamenaných vzorkách ich odpovedí (tests/fixtures/team_4)."""

import asyncio
import json
from pathlib import Path

import httpx
import psycopg
import pytest

from krakow_di.lake.adapters.team_4 import Team4RestAdapter
from krakow_di.lake.config import load_adapters
from krakow_di.lake.writer import write_message

FIX = Path(__file__).parent / "fixtures" / "team_4"
SOLD = (FIX / "tickets_sold.json").read_bytes()
AVAIL = (FIX / "theatre_availability_latest_limit4.json").read_bytes()
FLIGHTS_EMPTY = (FIX / "flights_empty.json").read_bytes()
FLIGHTS_SAMPLE = (FIX / "flights_sample.json").read_bytes()
SB = "https://zvopbhzdgtxtujobyfgs.supabase.co/rest/v1"
RENDER = "https://d-tov-in-inierstvo.onrender.com/api/flights"
THEATRE = [
    {"url": f"{SB}/theatre_availability_latest", "params": {"select": "*", "order": "instance_id"},
     "paginate": True},
    {"url": f"{SB}/tickets_sold", "params": {"select": "*", "order": "event"}, "paginate": True},
]


def rows(conn):
    return conn.execute("SELECT * FROM lake.message ORDER BY id").fetchall()


async def run_once(adapter, db_url):
    """Spustí adaptér, kým nie je uložená aspoň prvá správa a dobehne jeden prechod."""
    stop = asyncio.Event()
    async with await psycopg.AsyncConnection.connect(db_url) as c:
        async def sink(msg):
            await write_message(c, msg)
        task = asyncio.create_task(adapter.run(sink, stop))
        await asyncio.sleep(0.3)
        stop.set()
        await asyncio.wait_for(task, 10)


def test_theatre_sends_apikey_pages_and_stores_raw_bodies(migrated_db, conn):
    seen = []

    def handler(req):
        seen.append(req)
        view = req.url.path.rsplit("/", 1)[1]
        body = {"theatre_availability_latest": AVAIL, "tickets_sold": SOLD}[view]
        if req.url.params["offset"] != "0":
            body = b"[]"
        return httpx.Response(200, content=body, headers={"content-type": "application/json"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = Team4RestAdapter("team_4", "rest_theatre", THEATRE, interval_seconds=3600,
                               page_size=4, headers={"apikey": "tajny"}, client=client)
    asyncio.run(run_once(adapter, migrated_db))
    assert all(r.headers["apikey"] == "tajny" for r in seen)
    assert all(r.headers.get("authorization") is None for r in seen)  # Bearer by dal HTTP 401
    r = rows(conn)
    # plná stránka (4 riadky) -> pýta sa ďalšiu, tá je prázdna a neukladá sa
    assert [(x["channel"], x["source_ref"]["offset"]) for x in r][:2] == [
        ("rest_theatre", 0), ("rest_theatre", 0)]
    assert bytes(r[0]["payload_raw"]) == AVAIL and r[0]["payload_json"] == json.loads(AVAIL)
    assert bytes(r[1]["payload_raw"]) == SOLD
    assert r[0]["source_ref"]["url"].endswith("/theatre_availability_latest")
    assert any(q.url.params["offset"] == "4" for q in seen)  # stránkovanie pokračuje
    assert {q.url.params["order"] for q in seen} == {"instance_id", "event"}


def test_unchanged_snapshot_is_stored_once_and_change_is_new_message(migrated_db, conn):
    body = {"v": FLIGHTS_EMPTY}
    client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, content=body["v"],
                                 headers={"content-type": "application/json"})))
    ep = [{"url": RENDER}]
    for payload in (FLIGHTS_EMPTY, FLIGHTS_EMPTY, FLIGHTS_SAMPLE):
        body["v"] = payload
        adapter = Team4RestAdapter("team_4", "rest_flights", ep, interval_seconds=900,
                                   client=client)
        asyncio.run(run_once(adapter, migrated_db))
    r = rows(conn)
    assert len(r) == 2 and r[0]["channel"] == "rest_flights"
    assert r[0]["payload_json"] == {"team": "Tím 4", "flights": []}
    assert r[1]["payload_json"]["flights"][0]["destination"] == "DUB"
    assert r[0]["source_ref"]["offset"] is None  # bez stránkovania


def test_non_2xx_is_not_stored(migrated_db, conn):
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    adapter = Team4RestAdapter("team_4", "rest_flights", [{"url": RENDER}], client=client)
    asyncio.run(run_once(adapter, migrated_db))
    assert rows(conn) == []


def test_missing_key_raises_for_runner_restart(migrated_db):
    adapter = Team4RestAdapter("team_4", "rest_theatre", THEATRE, headers={"apikey": ""})
    with pytest.raises(RuntimeError, match="apikey"):
        asyncio.run(run_once(adapter, migrated_db))


def test_real_config_loads_team_4_without_key_and_with_key():
    adapters = {a.name: a for a in load_adapters("config/lake_sources.yaml", {})}
    theatre, flights = adapters["team_4/rest_theatre"], adapters["team_4/rest_flights"]
    assert theatre.headers == {"apikey": ""} and len(theatre.endpoints) == 2
    assert flights.headers == {} and flights.interval == 900
    with_key = {a.name: a for a in load_adapters(
        "config/lake_sources.yaml", {"TEAM4_SUPABASE_KEY": "abc"})}
    assert with_key["team_4/rest_theatre"].headers == {"apikey": "abc"}
