"""Adaptéry tímu 7 na zaznamenaných vzorkách (tests/fixtures/team_7)."""

import asyncio
import json
from pathlib import Path

import httpx
import psycopg
import pytest

from krakow_di.lake.adapters.team_7 import Team7RestAdapter, Team7SseAdapter
from krakow_di.lake.config import load_adapters
from krakow_di.lake.writer import write_message

FIX = Path(__file__).parent / "fixtures" / "team_7"
STREAM = (FIX / "stream_limit1.txt").read_bytes()
THEATRE = (FIX / "theatre_limit2.json").read_bytes()
URL = "https://university-data-api.artur-yevdokymov.workers.dev/api/stream?limit=100"
API = "https://university-data-api.artur-yevdokymov.workers.dev/api/theatre"


def rows(conn):
    return conn.execute("SELECT * FROM lake.message ORDER BY id").fetchall()


async def run_adapter(adapter, db_url):
    stop, results = asyncio.Event(), []
    async with await psycopg.AsyncConnection.connect(db_url) as c:
        async def sink(msg):
            results.append(await write_message(c, msg))
        await asyncio.wait_for(adapter.run(sink, stop), 30)
    return results


def test_sse_stores_raw_frames_skips_heartbeat_and_dedupes_reconnect(migrated_db, conn):
    def handler(req):
        return httpx.Response(200, content=STREAM, headers={"content-type": "text/event-stream"})

    async def run():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        # server spojenie zatvoril a adaptér sa pripojil znova: tie isté snímky
        adapter = Team7SseAdapter("team_7", URL, reconnect_seconds=0.01, client=client,
                                      max_connections=2)
        return await run_adapter(adapter, migrated_db)

    results = asyncio.run(run())
    r = rows(conn)
    events = [x["source_ref"]["event"] for x in r]
    assert events == ["ready", "snapshot", "warning", "snapshot", "snapshot", "warning",
                      "snapshot", "snapshot", "snapshot", "warning"]  # bez heartbeat
    assert results.count(True) == 10 and results.count(False) == 10  # druhé pripojenie = duplicity
    first = r[1]
    assert first["channel"] == "sse" and first["content_type"] == "text/event-stream"
    assert first["source_ref"]["url"] == URL
    body = json.loads(first["payload_raw"])  # surové bajty ostávajú bez zmeny
    assert (body["database"], body["table"]) == ("db1", "flight_observations")
    assert first["payload_json"] == body
    assert body["rows"][0]["source_table"] == "flight_observations"


def test_sse_http_error_is_raised_for_runner_backoff(migrated_db):
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    adapter = Team7SseAdapter("team_7", URL, client=client)
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(run_adapter(adapter, migrated_db))


def test_rest_pages_until_short_page_and_dedupes_unchanged_snapshots(migrated_db, conn):
    calls = []
    body = {"v": THEATRE}

    def handler(req):
        calls.append((req.url.params["limit"], req.url.params["offset"]))
        if req.url.params["offset"] == "0":
            return httpx.Response(200, content=body["v"],
                                  headers={"content-type": "application/json"})
        return httpx.Response(200, json={"success": True, "count": 0, "data": []})

    async def run(adapter_body):
        body["v"] = adapter_body
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        # page_size = 2 podľa vzorky: plná stránka -> pýta sa ďalšiu (offset 2), tá je prázdna
        adapter = Team7RestAdapter("team_7", API, interval_seconds=3600, page_size=2,
                                       client=client)
        stop = asyncio.Event()
        async with await psycopg.AsyncConnection.connect(migrated_db) as c:
            async def sink(msg):
                await write_message(c, msg)
                stop.set()
            await asyncio.wait_for(adapter.run(sink, stop), 30)

    asyncio.run(run(THEATRE))
    asyncio.run(run(THEATRE))  # ďalší beh s rovnakým obsahom: duplicita sa neuloží
    changed = json.loads(THEATRE)
    changed["data"][0]["available_total"] = 1
    asyncio.run(run(json.dumps(changed).encode()))  # zmena -> nová správa
    r = rows(conn)
    assert len(r) == 2
    assert bytes(r[0]["payload_raw"]) == THEATRE and r[0]["channel"] == "rest"
    assert r[0]["source_ref"]["offset"] == 0 and r[0]["payload_json"]["data"][0]["is_sold"] in (
        True, False)
    assert ("2", "0") in calls and r[1]["payload_json"]["data"][0]["available_total"] == 1


def test_rest_non_2xx_is_not_stored(migrated_db, conn):
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    adapter = Team7RestAdapter("team_7", API, interval_seconds=0.01, client=client)

    async def run():
        stop = asyncio.Event()
        async with await psycopg.AsyncConnection.connect(migrated_db):
            task = asyncio.create_task(adapter.run(lambda m: None, stop))
            await asyncio.sleep(0.1)
            stop.set()
            await task

    asyncio.run(run())
    assert rows(conn) == []


def test_real_config_loads_team_7_adapters():
    adapters = {a.name: a for a in load_adapters()}
    sse, rest = adapters["team_7/sse"], adapters["team_7/rest"]
    assert isinstance(sse, Team7SseAdapter) and sse.url.endswith("/api/stream?limit=100")
    assert isinstance(rest, Team7RestAdapter) and rest.page_size == 1000
