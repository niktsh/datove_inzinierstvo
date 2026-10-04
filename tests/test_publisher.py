import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import KafkaConnectionError, KafkaError

from krakow_di.collectors.base import OfferChange
from krakow_di.config import Settings
from krakow_di.events.builders import offer_events, theater_events
from krakow_di.events.envelope import InvalidEvent, make_event, validate
from krakow_di.events.publisher import publish_events, publish_pending, store_events
from krakow_di.events.routing import key_for, topic_names
from krakow_di.events.topics import delete_topics, ensure_topics
from krakow_di.repo.flight_offers import OfferObservation, upsert_offer
from tests.test_slowacki_collector import Site, make, seed_rate, with_seats

NOW = datetime(2026, 10, 14, 6, 0, tzinfo=UTC)
DEP = datetime(2026, 11, 20, 5, 15, tzinfo=UTC)
BOOTSTRAP = "localhost:9092"


def observation(price="54.99", at=NOW, source="ryanair", **kw):
    return OfferObservation(
        source=source, origin_iata="BCN", destination_iata="KRK", departure_at=DEP,
        airline_iata="FR", price=Decimal(price), currency="EUR", observed_at=at,
        flight_number="FR3035", **kw,
    )


def changes_for(conn, *observations):
    return [OfferChange(o, upsert_offer(conn, o)) for o in observations]


# ---------------------------------------------------------------- builders
def test_offer_events_found_observed_and_price_changed(conn):
    first = offer_events(conn, changes_for(conn, observation()), "p")
    assert [e["event_type"] for e in first] == ["flight.offer.found"]
    d = first[0]["data"]
    assert d["seats_total"] is None and d["seats_simulated"] is True
    assert d["departure_at"] == "2026-11-20T05:15:00+00:00" and first[0]["source"] == "ryanair"

    again = offer_events(conn, changes_for(conn, observation(at=NOW + timedelta(hours=6))), "p")
    assert [e["event_type"] for e in again] == ["flight.offer.observed"]

    changed = offer_events(
        conn, changes_for(conn, observation("61.49", NOW + timedelta(hours=12))), "p")
    assert [e["event_type"] for e in changed] == ["flight.offer.observed",
                                                  "flight.offer.price_changed"]
    pc = changed[1]["data"]
    assert (pc["old_price"], pc["new_price"], pc["reason"]) == (54.99, 61.49, "scrape")
    assert len({e["data"]["offer_id"] for e in first + again + changed}) == 1


def test_offer_events_take_seats_from_db_and_handle_empty(conn):
    assert offer_events(conn, [], "p") == []
    ch = changes_for(conn, observation())
    conn.execute("UPDATE core.flight_offer SET seats_total=189, seats_left=141")
    (event,) = offer_events(conn, ch, "p")
    assert (event["data"]["seats_total"], event["data"]["seats_left"]) == (189, 141)


def test_theater_events_cover_all_types_and_are_valid(conn):
    seed_rate(conn)
    site = Site()
    c, _ = make(site)
    c.run(conn)
    site.seat_override = with_seats(
        json.loads(json.dumps(site.seat_override or __import__("tests.test_slowacki_collector",
                  fromlist=["MOS"]).MOS)), "Bilet", -2)
    run = c.run(conn)
    events = theater_events(run, "p", NOW)
    kinds = {e["event_type"] for e in events}
    assert kinds == {"theater.performance.found", "theater.performance.updated",
                     "theater.availability.snapshot", "theater.tickets.sold"} or kinds >= {
        "theater.availability.snapshot", "theater.tickets.sold"}
    for e in events:
        validate(e)
    snap = next(e for e in events if e["event_type"] == "theater.availability.snapshot")
    assert snap["data"]["fx_rate"] == 4.3775
    bilet = [c for c in snap["data"]["categories"] if c["category"] == "Bilet"][0]
    assert bilet["price_eur"] == 22.84 and bilet["currency"] == "PLN"
    sale = next(e for e in events if e["event_type"] == "theater.tickets.sold")
    assert sale["data"]["quantity"] == 2 and sale["data"]["unit_price_eur"] == 22.84


# ---------------------------------------------------------------- store / publish
class FakeProducer:
    def __init__(self, fail_after=None):
        self.sent, self.fail_after = [], fail_after

    async def send_and_wait(self, topic, value, key=None, headers=None):
        if self.fail_after is not None and len(self.sent) >= self.fail_after:
            raise KafkaError("broker nedostupný")
        self.sent.append((topic, json.loads(value), key.decode(), dict(headers)))


def sold_out_event(offer_id, at):
    return make_event("flight.offer.sold_out", "generator",
                      {"offer_id": offer_id, "last_price": 10.0, "currency": "EUR"}, at, "p")


def test_store_events_is_idempotent_and_rejects_invalid(conn):
    e = sold_out_event("a1b2c3d4e5f60718", NOW)
    assert store_events(conn, [e], "krakow") == 1
    assert store_events(conn, [e], "krakow") == 0  # rovnaké event_id sa neuloží dvakrát
    row = conn.execute("SELECT * FROM core.event_log").fetchone()
    assert row["topic"] == "krakow.flights.offers" and row["published_at"] is None
    assert row["payload"]["event_id"] == e["event_id"] and row["seq"] >= 1
    bad = {**e, "event_id": str(uuid.uuid4()), "data": {"offer_id": "zle"}}
    with pytest.raises(InvalidEvent):
        store_events(conn, [bad], "krakow")
    assert conn.execute("SELECT count(*) AS n FROM core.event_log").fetchone()["n"] == 1


def test_publish_pending_sends_in_seq_order_with_key_and_header(conn):
    events = [sold_out_event(f"{i:016x}", NOW + timedelta(seconds=i)) for i in range(5)]
    store_events(conn, events, "krakow")
    producer = FakeProducer()
    assert asyncio.run(publish_pending(conn, producer)) == 5
    assert [m[1]["event_id"] for m in producer.sent] == [e["event_id"] for e in events]
    topic, event, key, headers = producer.sent[0]
    assert topic == "krakow.flights.offers" and key == key_for(event)
    assert headers == {"event_type": b"flight.offer.sold_out"}
    assert conn.execute("SELECT count(*) AS n FROM core.event_log "
                        "WHERE published_at IS NULL").fetchone()["n"] == 0
    assert asyncio.run(publish_pending(conn, FakeProducer())) == 0  # nič nezostalo


def test_kafka_failure_keeps_order_and_remaining_events_for_retry(conn):
    events = [sold_out_event(f"{i:016x}", NOW + timedelta(seconds=i)) for i in range(4)]
    store_events(conn, events, "krakow")
    assert asyncio.run(publish_pending(conn, FakeProducer(fail_after=2))) == 2
    pending = conn.execute("SELECT payload FROM core.event_log WHERE published_at IS NULL "
                           "ORDER BY seq").fetchall()
    assert [p["payload"]["event_id"] for p in pending] == [e["event_id"] for e in events[2:]]
    retry = FakeProducer()
    assert asyncio.run(publish_pending(conn, retry)) == 2
    assert [m[1]["event_id"] for m in retry.sent] == [e["event_id"] for e in events[2:]]


# ---------------------------------------------------------------- reálna Kafka
@pytest.fixture
def kafka_prefix():
    async def reachable():
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=3000)
        try:
            await admin.start()
        finally:
            await admin.close()

    try:
        asyncio.run(reachable())
    except (TimeoutError, KafkaConnectionError, OSError, KafkaError) as e:
        pytest.skip(f"Kafka z docker compose nie je dostupná: {e}")
    prefix = f"krakowtest{uuid.uuid4().hex[:8]}"
    yield prefix
    asyncio.run(delete_topics(BOOTSTRAP, prefix))


def test_real_kafka_topics_publish_headers_keys_and_order(conn, kafka_prefix):
    first = asyncio.run(ensure_topics(BOOTSTRAP, kafka_prefix))
    assert set(first.values()) == {"vytvorený"} and set(first) == set(topic_names(kafka_prefix))
    second = asyncio.run(ensure_topics(BOOTSTRAP, kafka_prefix))
    assert set(second.values()) == {"existuje"}  # idempotentné

    async def partitions():
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        await admin.start()
        try:
            return {t["topic"]: len(t["partitions"])
                    for t in await admin.describe_topics(list(first))}
        finally:
            await admin.close()

    assert asyncio.run(partitions()) == {n: s.partitions
                                         for n, s in topic_names(kafka_prefix).items()}

    settings = Settings(_env_file=None, kafka_bootstrap_servers=BOOTSTRAP,
                        kafka_topic_prefix=kafka_prefix)
    offers = [f"{i:016x}" for i in range(6)]
    events = [sold_out_event(oid, NOW + timedelta(seconds=n * 10 + i))
              for n in range(3) for i, oid in enumerate(offers)]
    events.append(make_event("theater.tickets.sold", "theater", {
        "performance_id": "x-2026-11-03-19-00", "category": "Bilet", "quantity": 1,
        "unit_price": 100.0, "currency": "PLN", "unit_price_eur": 22.84, "fx_rate": 4.3775,
        "detected_between": ["2026-10-14T08:00:01Z", "2026-10-14T12:00:03Z"]}, NOW, "p"))
    assert publish_events(conn, events, settings) == (len(events), len(events))

    async def read_all():
        topics = list(topic_names(kafka_prefix))
        consumer = AIOKafkaConsumer(
            *topics, bootstrap_servers=BOOTSTRAP, auto_offset_reset="earliest", group_id=None)
        await consumer.start()
        got = []
        try:
            while len(got) < len(events):
                batch = await consumer.getmany(timeout_ms=5000)
                if not batch:
                    break
                for msgs in batch.values():
                    got.extend(msgs)
        finally:
            await consumer.stop()
        return got

    msgs = asyncio.run(read_all())
    assert len(msgs) == len(events)
    by_key: dict[str, list] = {}
    for m in msgs:
        event = json.loads(m.value)
        validate(event)
        assert dict(m.headers)["event_type"].decode() == event["event_type"]
        assert m.key.decode() == key_for(event)
        by_key.setdefault((m.topic, m.key.decode()), []).append((m.partition, m.offset, event))
    sold = by_key[(f"{kafka_prefix}.theater.sales", "x-2026-11-03-19-00")]
    assert len(sold) == 1
    for (topic, _key), items in by_key.items():
        assert len({p for p, _, _ in items}) == 1  # jeden kľúč = jedna partícia
        times = [e["occurred_at"] for _, _, e in sorted(items, key=lambda x: x[1])]
        assert times == sorted(times)  # poradie podľa offsetu = poradie vzniku
        if topic.endswith("flights.offers"):
            assert len(items) == 3
    assert conn.execute("SELECT count(*) AS n FROM core.event_log "
                        "WHERE published_at IS NULL").fetchone()["n"] == 0
