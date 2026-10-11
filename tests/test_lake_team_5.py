"""Tím 5: S3 adaptér (letenky) a webhook (divadlo) na vzorkách z tests/fixtures/team_5."""

import asyncio
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import psycopg
import pytest
from fastapi.testclient import TestClient

from krakow_di.api.app import create_app
from krakow_di.config import Settings
from krakow_di.lake.adapters.team_5 import Team5S3Adapter
from krakow_di.lake.config import load_adapters
from krakow_di.lake.writer import write_message

FIX = Path(__file__).parent / "fixtures" / "team_5"
RYANAIR = (FIX / "ryanair_batch.json").read_bytes()
ESKY = (FIX / "esky_batch.json").read_bytes()
KEY_R = "bronze/domain=flights/source=ryanair/year=2026/month=10/day=03/hour=16/batch_a.json"
KEY_E = "bronze/domain=flights/source=esky/year=2026/month=10/day=04/hour=20/batch_b.json"


class FakeS3:
    """Minimálna náhrada boto3 klienta: list_objects_v2 po stránkach a get_object."""

    def __init__(self, objects: dict[str, bytes], page_size: int = 1):
        self.objects, self.page_size, self.gets = objects, page_size, []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        outer = self

        class P:
            def paginate(self, Bucket, Prefix):
                items = [{"Key": k, "ETag": f'"{hash(v) & 0xffffff:x}"', "Size": len(v),
                          "LastModified": datetime(2026, 10, 11, 6, i, tzinfo=UTC)}
                         for i, (k, v) in enumerate(outer.objects.items())
                         if k.startswith(Prefix)]
                for i in range(0, len(items), outer.page_size):
                    yield {"Contents": items[i:i + outer.page_size]}
        return P()

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        return {"Body": io.BytesIO(self.objects[Key]), "ContentType": "application/json"}


def rows(conn):
    return conn.execute("SELECT * FROM lake.message ORDER BY id").fetchall()


async def run_once(adapter, db_url):
    stop = asyncio.Event()
    async with await psycopg.AsyncConnection.connect(db_url) as c:
        async def sink(msg):
            await write_message(c, msg)
        task = asyncio.create_task(adapter.run(sink, stop))
        await asyncio.sleep(0.5)
        stop.set()
        await asyncio.wait_for(task, 10)


def test_s3_stores_each_object_raw_and_does_not_redownload_known(migrated_db, conn):
    fake = FakeS3({KEY_R: RYANAIR, KEY_E: ESKY, "bronze/domain=theatre/x.json": b"{}"})
    adapter = Team5S3Adapter("team_5", "https://r2", "data-lake", interval_seconds=0.05,
                             client=fake, concurrency=2)
    asyncio.run(run_once(adapter, migrated_db))
    r = rows(conn)
    assert [x["source_ref"]["key"] for x in r] == [KEY_R, KEY_E]  # iba prefix letenky
    assert all(x["channel"] == "s3" and x["source_ref"]["bucket"] == "data-lake" for x in r)
    assert bytes(r[0]["payload_raw"]) == RYANAIR and r[0]["payload_json"] == json.loads(RYANAIR)
    assert r[0]["content_type"] == "application/json"
    assert sorted(fake.gets) == sorted([KEY_R, KEY_E])  # opakované prechody nesťahujú znova


def test_s3_new_object_is_picked_up_on_next_poll(migrated_db, conn):
    fake = FakeS3({KEY_R: RYANAIR})
    adapter = Team5S3Adapter("team_5", "https://r2", "data-lake", interval_seconds=0.1,
                             client=fake)

    async def run():
        stop = asyncio.Event()
        async with await psycopg.AsyncConnection.connect(migrated_db) as c:
            async def sink(msg):
                await write_message(c, msg)
            task = asyncio.create_task(adapter.run(sink, stop))
            await asyncio.sleep(0.4)
            fake.objects[KEY_E] = ESKY
            await asyncio.sleep(0.5)
            stop.set()
            await asyncio.wait_for(task, 10)

    asyncio.run(run())
    assert [x["source_ref"]["key"] for x in rows(conn)] == [KEY_R, KEY_E]


def test_s3_missing_keys_raise_for_runner_restart(migrated_db):
    adapter = Team5S3Adapter("team_5", "https://r2", "data-lake")
    with pytest.raises(RuntimeError, match="kľúče"):
        asyncio.run(run_once(adapter, migrated_db))


def test_real_config_loads_team_5_without_and_with_keys():
    adapters = {a.name: a for a in load_adapters("config/lake_sources.yaml", {})}
    a = adapters["team_5/s3"]
    assert a.bucket == "data-lake" and a.prefix == "bronze/domain=flights/"
    assert a.access_key == "" and a.interval == 1800
    keys = {"TEAM5_R2_ACCESS_KEY_ID": "id", "TEAM5_R2_SECRET_ACCESS_KEY": "secret"}
    b = {x.name: x for x in load_adapters("config/lake_sources.yaml", keys)}["team_5/s3"]
    assert (b.access_key, b.secret_key) == ("id", "secret")


# ---------------------------------------------------------------- webhook
EVENT = {"event_id": "550e8400-e29b-41d4-a716-446655440000",
         "update_timestamp": "2026-10-09T19:00:00Z", "title": "Labutí jezero",
         "tickets_sold": 4, "seats_remaining": 120, "status": "active"}


def webhook_client(url: str, token: str = "tajny"):
    u = urlparse(url)
    s = Settings(_env_file=None, postgres_host=u.hostname, postgres_port=u.port,
                 postgres_user=u.username, postgres_password=u.password,
                 postgres_db=u.path.lstrip("/"), team5_webhook_token=token)
    return TestClient(create_app(s, start_gateway=False))


def test_webhook_stores_body_as_is_and_ignores_duplicates(migrated_db, conn):
    body = json.dumps(EVENT, ensure_ascii=False).encode()
    with webhook_client(migrated_db) as c:
        r1 = c.post("/webhooks/team_5/tajny", content=body,
                    headers={"content-type": "application/json"})
        r2 = c.post("/webhooks/team_5/tajny", content=body,
                    headers={"content-type": "application/json"})
        r3 = c.post("/webhooks/team_5/tajny", content=b"nie je json")
    assert (r1.status_code, r1.json()) == (200, {"ok": True, "stored": True})
    assert r2.json() == {"ok": True, "stored": False} and r3.json()["stored"] is True
    r = rows(conn)
    assert [x["channel"] for x in r] == ["webhook", "webhook"] and r[0]["team"] == "team_5"
    assert bytes(r[0]["payload_raw"]) == body and r[0]["payload_json"] == EVENT
    assert r[1]["payload_json"] is None and bytes(r[1]["payload_raw"]) == b"nie je json"


def test_webhook_rejects_wrong_token_disabled_empty_and_oversized(migrated_db, conn):
    with webhook_client(migrated_db) as c:
        assert c.post("/webhooks/team_5/zly", content=b"{}").status_code == 404
        assert c.post("/webhooks/team_5/tajny", content=b"").status_code == 400
        big = b"x" * (256 * 1024 + 1)
        assert c.post("/webhooks/team_5/tajny", content=big).status_code == 413
        assert c.get("/webhooks/team_5/tajny").status_code == 405
    with webhook_client(migrated_db, token="") as c:  # vypnuté: ani prázdny token neprejde
        assert c.post("/webhooks/team_5/", content=b"{}").status_code in (404, 405)
        assert c.post("/webhooks/team_5/tajny", content=b"{}").status_code == 404
    assert rows(conn) == []
