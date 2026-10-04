import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import psycopg
import pytest
import websockets
import yaml
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError, KafkaError

from krakow_di.config import Settings
from krakow_di.events.envelope import make_event
from krakow_di.events.publisher import publish_events
from krakow_di.lake.adapters.base import Adapter
from krakow_di.lake.adapters.kafka import KafkaAdapter
from krakow_di.lake.adapters.rest import RestPollAdapter
from krakow_di.lake.adapters.sse import SseAdapter, parse_sse_lines
from krakow_di.lake.adapters.websocket import WebSocketAdapter
from krakow_di.lake.config import LakeConfigError, build_adapter, load_adapters
from krakow_di.lake.message import LakeMessage
from krakow_di.lake.runner import AdapterStats, run_adapter, run_all
from krakow_di.lake.writer import write_message
from tests.test_publisher import BOOTSTRAP, sold_out_event

NOW = datetime(2026, 10, 14, 6, 0, tzinfo=UTC)


def rows(conn, where="true"):
    return conn.execute(f"SELECT * FROM lake.message WHERE {where} ORDER BY id").fetchall()


async def collect(adapter, db_url, until, timeout=30.0):
    """Spustí adaptér so skutočným zápisom do DB, kým nie je uložených `until` správ."""
    stop = asyncio.Event()
    stats = AdapterStats()
    async with await psycopg.AsyncConnection.connect(db_url) as conn:
        async def sink(msg):
            await write_message(conn, msg)
            stats.written += 1
            if stats.written >= until:
                stop.set()
        await asyncio.wait_for(adapter.run(sink, stop), timeout)
    return stats


# ---------------------------------------------------------------- writer
def test_writer_stores_raw_bytes_json_and_ignores_duplicates(migrated_db, conn):
    async def run():
        async with await psycopg.AsyncConnection.connect(migrated_db) as c:
            ref = {"topic": "t", "partition": 0, "offset": 5}
            ok = [
                await write_message(c, LakeMessage("team_01", "kafka", b'{"a": 1}', ref,
                                                   "application/json")),
                await write_message(c, LakeMessage("team_01", "kafka", b'{"a": 1}', ref)),
                await write_message(c, LakeMessage("team_02", "kafka", b'{"a": 1}', ref)),
                await write_message(c, LakeMessage("team_01", "ws", b"\xff\xfe binarne")),
                await write_message(c, LakeMessage("team_01", "ws", b"\xff\xfe binarne")),
                await write_message(c, LakeMessage("team_01", "ws", b"[1,2,3]")),
                await write_message(c, LakeMessage("team_01", "ws", b'{"nul": "\\u0000"}')),
                await write_message(c, LakeMessage("team_01", "ws", b"nie json")),
            ]
        return ok

    ok = asyncio.run(run())
    assert ok == [True, False, True, True, True, True, True, True]
    r = rows(conn)
    assert len(r) == 7
    assert r[0]["payload_json"] == {"a": 1} and r[0]["content_type"] == "application/json"
    assert bytes(r[0]["payload_raw"]) == b'{"a": 1}' and r[0]["source_ref"]["offset"] == 5
    binary = [x for x in r if bytes(x["payload_raw"]).startswith(b"\xff")]
    assert len(binary) == 2 and all(x["payload_json"] is None for x in binary)  # bez source_ref
    assert [x["payload_json"] for x in r if bytes(x["payload_raw"]) == b"[1,2,3]"] == [[1, 2, 3]]
    nul = [x for x in r if b"nul" in bytes(x["payload_raw"])][0]
    assert nul["payload_json"] is None and b"\\u0000" in bytes(nul["payload_raw"])  # surové ostáva


def test_message_payload_json():
    assert LakeMessage("t", "c", b'{"x":1}').payload_json() == '{"x":1}'
    assert LakeMessage("t", "c", b"zle").payload_json() is None
    assert LakeMessage("t", "c", "žluťoučký".encode("latin-1", "ignore")).payload_json() is None


# ---------------------------------------------------------------- SSE
def test_parse_sse_lines():
    assert parse_sse_lines(["id: 7", "event: x", "data: {\"a\":1}"]) == {
        "id": "7", "event": "x", "data": '{"a":1}'}
    assert parse_sse_lines([": keepalive", "data: a", "data: b"]) == {"data": "a\nb"}
    assert parse_sse_lines([": iba komentár"]) is None and parse_sse_lines(["retry: 3000"]) is None


def test_sse_adapter_saves_frames_and_resumes_with_last_event_id(migrated_db, conn):
    seen_headers = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen_headers.append(req.headers.get("last-event-id"))
        if len(seen_headers) == 1:
            body = ("retry: 3000\n\nid: 1\nevent: a\ndata: {\"n\":1}\n\n"
                    ": keepalive\n\nid: 2\ndata: surový text\n\n")
        else:
            body = "id: 3\ndata: {\"n\":3}\n\n"
        return httpx.Response(200, content=body.encode(),
                              headers={"content-type": "text/event-stream"})

    async def run():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = SseAdapter("team_04", "https://t4.example/stream", client=client)
        stop = asyncio.Event()
        async with await psycopg.AsyncConnection.connect(migrated_db) as c:
            async def sink(msg):
                await write_message(c, msg)
            with pytest.raises(ConnectionError):  # server spojenie ukončil -> runner reštartuje
                await adapter.run(sink, stop)
            with pytest.raises(ConnectionError):
                await adapter.run(sink, stop)

    asyncio.run(run())
    assert seen_headers == [None, "2"]  # po výpadku pokračuje od posledného id
    r = rows(conn)
    assert [x["source_ref"]["id"] for x in r] == ["1", "2", "3"]
    assert r[0]["source_ref"] == {"url": "https://t4.example/stream", "id": "1", "event": "a"}
    assert r[0]["payload_json"] == {"n": 1} and r[1]["payload_json"] is None
    assert r[1]["content_type"] == "text/event-stream" and r[1]["channel"] == "sse"


# ---------------------------------------------------------------- REST
def test_rest_adapter_stores_snapshots_and_skips_errors(migrated_db, conn):
    calls = []

    def handler(req):
        calls.append(str(req.url))
        if "zle" in str(req.url):
            return httpx.Response(500, text="chyba")
        return httpx.Response(200, json={"items": [1, 2]})

    async def run():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        urls = ["https://t6/ok", "https://t6/zle"]
        adapter = RestPollAdapter("team_06", urls, 0.05, client=client)
        return await collect(adapter, migrated_db, until=2)

    asyncio.run(run())
    r = rows(conn)
    assert len(r) == 2 and all(x["source_ref"]["url"] == "https://t6/ok" for x in r)
    assert r[0]["payload_json"] == {"items": [1, 2]} and r[0]["channel"] == "rest"
    assert r[0]["content_type"].startswith("application/json")
    assert r[0]["source_ref"]["fetched_at"] != r[1]["source_ref"]["fetched_at"]
    assert any("zle" in c for c in calls)


# ---------------------------------------------------------------- WebSocket
def test_websocket_adapter_text_and_binary(migrated_db, conn):
    async def run():
        async def handler(ws):
            await ws.send('{"x": 1}')
            await ws.send(b"\x00\x01\x02")
            await asyncio.sleep(5)

        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            adapter = WebSocketAdapter("team_05", f"ws://127.0.0.1:{port}")
            return await collect(adapter, migrated_db, until=2, timeout=15)

    asyncio.run(run())
    r = rows(conn)
    assert [x["channel"] for x in r] == ["ws", "ws"] and all(x["source_ref"] is None for x in r)
    assert r[0]["payload_json"] == {"x": 1} and r[0]["content_type"] == "text/plain"
    assert bytes(r[1]["payload_raw"]) == b"\x00\x01\x02"
    assert r[1]["content_type"] == "application/octet-stream" and r[1]["payload_json"] is None


# ---------------------------------------------------------------- Kafka
@pytest.fixture
def kafka_topics():
    """Dočasné topiky cudzieho tímu na skutočnom brokere (vytvoria sa a po teste sa zmažú)."""
    prefix = f"lakeT{uuid.uuid4().hex[:8]}"
    names = [f"{prefix}.a", f"{prefix}.b"]

    async def setup():
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=3000)
        await admin.start()
        try:
            await admin.create_topics([NewTopic(n, 1, 1) for n in names])
        finally:
            await admin.close()

    try:
        asyncio.run(setup())
    except (KafkaConnectionError, OSError, KafkaError, TimeoutError) as e:
        pytest.skip(f"Kafka z docker compose nie je dostupná: {e}")
    yield prefix, names

    async def teardown():
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        await admin.start()
        try:
            await admin.delete_topics(names)
        finally:
            await admin.close()

    asyncio.run(teardown())


async def produce(topic, values, headers=None):
    p = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await p.start()
    try:
        for v in values:
            await p.send_and_wait(topic, v, headers=headers)
    finally:
        await p.stop()


def test_kafka_adapter_stores_with_source_ref_and_resumes_without_duplicates(
        migrated_db, conn, kafka_topics):
    prefix, (topic_a, topic_b) = kafka_topics
    group = f"lake-test-{uuid.uuid4().hex[:6]}"

    async def run():
        await produce(topic_a, [b'{"n": 1}', b"\xff surovy", b'{"n": 3}'],
                      headers=[("content-type", b"application/json")])
        await produce(topic_b, [b'{"b": 1}'])
        adapter = KafkaAdapter("team_03", BOOTSTRAP, group, topics=[topic_a, topic_b])
        first = await collect(adapter, migrated_db, until=4)
        # druhý beh tej istej skupiny: nové správy, staré sa nečítajú znova
        await produce(topic_a, [b'{"n": 4}'])
        again = KafkaAdapter("team_03", BOOTSTRAP, group, topics=[topic_a, topic_b])
        second = await collect(again, migrated_db, until=1)
        return first.written, second.written

    assert asyncio.run(run()) == (4, 1)
    r = rows(conn)
    assert len(r) == 5
    refs = {(x["source_ref"]["topic"], x["source_ref"]["offset"]) for x in r}
    assert refs == {(topic_a, 0), (topic_a, 1), (topic_a, 2), (topic_a, 3), (topic_b, 0)}
    raw = [x for x in r if bytes(x["payload_raw"]).startswith(b"\xff")][0]
    assert raw["payload_json"] is None and raw["channel"] == "kafka"
    assert raw["content_type"] == "application/json"  # hlavička content-type sa zachová
    assert [x["payload_json"] for x in r if x["source_ref"]["topic"] == topic_b] == [{"b": 1}]


def test_kafka_crash_before_commit_redelivers_and_dedupes(migrated_db, conn, kafka_topics):
    _prefix, (topic_a, _topic_b) = kafka_topics
    group = f"lake-test-{uuid.uuid4().hex[:6]}"

    async def run():
        await produce(topic_a, [b'{"n": %d}' % i for i in range(5)])
        adapter = KafkaAdapter("team_03", BOOTSTRAP, group, topics=[topic_a])
        stop = asyncio.Event()
        async with await psycopg.AsyncConnection.connect(migrated_db) as c:
            count = 0

            async def crashing_sink(msg):
                nonlocal count
                await write_message(c, msg)
                count += 1
                if count == 3:
                    raise RuntimeError("pád po zápise, pred potvrdením offsetu")

            with pytest.raises(RuntimeError):
                await asyncio.wait_for(adapter.run(crashing_sink, stop), 30)
        # reštart: správy sa prečítajú znova, duplicity odstráni source_ref
        replay = KafkaAdapter("team_03", BOOTSTRAP, group, topics=[topic_a])
        stop2 = asyncio.Event()
        seen = 0
        async with await psycopg.AsyncConnection.connect(migrated_db) as c:
            async def sink(msg):
                nonlocal seen
                await write_message(c, msg)
                seen += 1
                if seen >= 5:
                    stop2.set()
            await asyncio.wait_for(replay.run(sink, stop2), 30)
        return seen

    assert asyncio.run(run()) == 5  # po páde sa dávka prečítala znova od začiatku (aj 1–3)
    r = rows(conn)
    assert len(r) == 5  # a v lake je každá správa práve raz
    assert sorted(x["source_ref"]["offset"] for x in r) == [0, 1, 2, 3, 4]


def test_own_events_end_to_end_with_regex_subscription(migrated_db, conn):
    """Naše udalosti sa cez adaptér `lake-self` dostanú do lake presne tak, ako boli odoslané."""
    from krakow_di.events.topics import delete_topics, ensure_topics

    prefix = f"krakowtest{uuid.uuid4().hex[:8]}"
    settings = Settings(_env_file=None, kafka_bootstrap_servers=BOOTSTRAP,
                        kafka_topic_prefix=prefix)
    try:
        asyncio.run(ensure_topics(BOOTSTRAP, prefix))
    except (KafkaConnectionError, OSError, KafkaError, TimeoutError) as e:
        pytest.skip(f"Kafka z docker compose nie je dostupná: {e}")
    try:
        events = [sold_out_event(f"{i:016x}", NOW + timedelta(seconds=i)) for i in range(3)]
        events.append(make_event("theater.tickets.sold", "theater", {
            "performance_id": "x-2026-11-03-19-00", "category": "Bilet", "quantity": 1,
            "unit_price": 100.0, "currency": "PLN", "unit_price_eur": 22.84, "fx_rate": 4.3775,
            "detected_between": ["2026-10-14T08:00:01Z", "2026-10-14T12:00:03Z"]}, NOW, "p"))
        assert publish_events(conn, events, settings) == (4, 4)
        adapter = KafkaAdapter("tuke-krakow", BOOTSTRAP, "lake-self-test",
                               topics_regex=rf"^{prefix}\..*")
        asyncio.run(collect(adapter, migrated_db, until=4))
    finally:
        asyncio.run(delete_topics(BOOTSTRAP, prefix))
    r = rows(conn, "team = 'tuke-krakow'")
    assert len(r) == 4
    stored = {x["payload_json"]["event_id"]: x for x in r}
    assert set(stored) == {e["event_id"] for e in events}
    for e in events:  # uložené presne to, čo sme odoslali
        assert stored[e["event_id"]]["payload_json"] == e
    assert {x["source_ref"]["topic"] for x in r} == {f"{prefix}.flights.offers",
                                                    f"{prefix}.theater.sales"}


# ---------------------------------------------------------------- konfigurácia
def write_cfg(tmp_path, sources):
    p = tmp_path / "lake.yaml"
    p.write_text(yaml.safe_dump({"sources": sources}), encoding="utf-8")
    return p


def test_load_adapters_env_substitution_secrets_and_disabled(tmp_path):
    cfg = write_cfg(tmp_path, [
        {"team": "a", "type": "kafka", "bootstrap": "${BROKER:-localhost:9092}",
         "topics": ["x"], "group_id": "team-05-lake", "security_protocol": "SASL_SSL",
         "sasl_mechanism": "SCRAM-SHA-512", "username_env": "A_USER", "password_env": "A_PASS"},
        {"team": "b", "type": "sse", "url": "https://${HOST}/stream", "token_env": "B_TOKEN"},
        {"team": "c", "type": "rest", "urls": ["https://c/x"], "interval_seconds": 60},
        {"team": "d", "type": "ws", "url": "wss://d/ws", "enabled": False},
        {"team": "e", "type": "custom", "class": "tests.lake_custom_adapter:DemoAdapter",
         "username_env": "A_USER", "extra": 5},
    ])
    env = {"HOST": "b.example", "A_USER": "u", "A_PASS": "p", "B_TOKEN": "tok"}
    adapters = load_adapters(cfg, env)
    assert [a.name for a in adapters] == ["a/kafka", "b/sse", "c/rest", "e/demo"]
    a, b, c, e = adapters
    assert a.bootstrap == "localhost:9092" and a.auth["sasl_plain_username"] == "u"
    assert a.auth["sasl_plain_password"] == "p" and a.auth["security_protocol"] == "SASL_SSL"
    assert b.url == "https://b.example/stream" and b.headers == {"Authorization": "Bearer tok"}
    assert c.interval == 60 and e.username == "u" and e.config == {"extra": 5}


def test_load_adapters_errors(tmp_path):
    with pytest.raises(LakeConfigError):  # chýbajúca premenná
        load_adapters(write_cfg(tmp_path, [
            {"team": "a", "type": "sse", "url": "https://${NIE_JE}/s"}]), {})
    with pytest.raises(LakeConfigError):  # chýbajúce heslo v .env
        load_adapters(write_cfg(tmp_path, [
            {"team": "a", "type": "kafka", "bootstrap": "x", "topics": ["t"], "group_id": "g",
             "password_env": "NIE_JE"}]), {})
    with pytest.raises(LakeConfigError):  # neznámy typ
        build_adapter({"team": "a", "type": "mqtt"}, {})
    with pytest.raises(LakeConfigError):  # duplicitný tím/kanál
        load_adapters(write_cfg(tmp_path, [
            {"team": "a", "type": "sse", "url": "u"},
            {"team": "a", "type": "sse", "url": "v"}]), {})
    with pytest.raises(ValueError):  # kafka bez topikov
        build_adapter({"team": "a", "type": "kafka", "bootstrap": "x", "group_id": "g"}, {})


def test_real_config_loads_own_source():
    adapters = load_adapters("config/lake_sources.yaml", {})
    assert [a.name for a in adapters] == ["tuke-krakow/kafka"]
    assert adapters[0].group_id == "lake-self" and adapters[0].topics_regex == r"^krakow\..*"


# ---------------------------------------------------------------- runner
class FlakyAdapter(Adapter):
    channel = "demo"

    def __init__(self, team, fail_times=2, messages=2):
        self.team, self.fail_times, self.messages, self.attempts = team, fail_times, messages, 0

    async def run(self, sink, stop):
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise ConnectionError(f"pokus {self.attempts} zlyhal")
        for i in range(self.messages):
            await sink(LakeMessage(self.team, self.channel, b'{"i": %d}' % i,
                                   {"attempt": self.attempts, "i": i}))
        await stop.wait()


def test_runner_restarts_failed_adapter_and_isolates_failures(migrated_db, conn):
    class AlwaysBroken(Adapter):
        team, channel = "rozbity", "demo"

        async def run(self, sink, stop):
            raise OSError("zdroj nedostupný")

    async def run():
        stop = asyncio.Event()
        good, broken = FlakyAdapter("dobry"), AlwaysBroken()
        task = asyncio.create_task(run_all([good, broken], migrated_db, stop, 0.01, 0.05))
        for _ in range(100):
            await asyncio.sleep(0.05)
            if conn.execute("SELECT count(*) AS n FROM lake.message").fetchone()["n"] >= 2:
                break
        stop.set()
        return await asyncio.wait_for(task, 10), good

    stats, good = asyncio.run(run())
    assert good.attempts == 3 and stats["dobry/demo"].restarts >= 2
    assert stats["dobry/demo"].written == 2 and stats["dobry/demo"].last_error.startswith("Conn")
    assert stats["rozbity/demo"].restarts >= 2 and "zdroj nedostupný" in stats[
        "rozbity/demo"].last_error and stats["rozbity/demo"].written == 0
    assert len(rows(conn, "team = 'dobry'")) == 2


def test_runner_stops_promptly_on_stop(migrated_db):
    async def run():
        stop = asyncio.Event()
        stats = AdapterStats()
        task = asyncio.create_task(run_adapter(FlakyAdapter("x", fail_times=0, messages=0),
                                               migrated_db, stop, stats, 0.01, 0.05))
        await asyncio.sleep(0.3)
        stop.set()
        await asyncio.wait_for(task, 3)
        return stats

    assert asyncio.run(run()).restarts == 0


# ---------------------------------------------------------------- report
def test_lake_stats_report(conn):
    from tools.lake_stats import QUERY, format_report

    assert format_report([], 24) == "data lake je zatiaľ prázdny"
    conn.execute(
        "INSERT INTO lake.message (team, channel, received_at, payload_raw, payload_json,"
        " source_ref)"
        " VALUES ('team_01','kafka', now(), '\\x7b7d', '{}', '{\"o\":1}'),"
        "        ('team_01','kafka', now() - interval '3 days', 'abc', NULL, '{\"o\":2}'),"
        "        ('team_02','sse', now(), '\\x7b7d', '{}', NULL)")
    conn.commit()
    report = format_report(conn.execute(QUERY, (24,)).fetchall(), 24)
    lines = report.splitlines()
    t1 = next(line for line in lines if line.startswith("team_01"))
    assert t1.split()[2:5] == ["1", "2", "1"]  # za 24 h 1, spolu 2, nie JSON 1
    assert "team_02" in report and "spolu: 3 správ od 2 tímov" in report
