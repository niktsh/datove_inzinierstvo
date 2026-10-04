"""Uloženie udalostí do core.event_log a odoslanie do Kafky.

Poradie: najprv validácia a zápis do `core.event_log` (published_at = NULL), potom odoslanie
do Kafky v poradí `seq`; po potvrdení brokerom sa nastaví `published_at`. Ak Kafka nie je
dostupná, udalosti zostanú neodoslané a odošlú sa pri ďalšom behu (`publish_pending`).
Nejde o plný transactional outbox (rozhodnutie v ARCHITEKTURA.md), len o minimálnu poistku.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime

import psycopg
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from psycopg.types.json import Jsonb

from krakow_di.config import Settings, get_settings
from krakow_di.events.envelope import validate
from krakow_di.events.routing import key_for, topic_for

log = logging.getLogger(__name__)


def store_events(conn: psycopg.Connection, events: list[dict], prefix: str) -> int:
    """Zvaliduje a zapíše udalosti do event_log (duplicitné event_id sa ignorujú)."""
    for event in events:
        validate(event)
    stored = 0
    for event in events:
        row = conn.execute(
            "INSERT INTO core.event_log (event_id, event_type, topic, occurred_at, payload) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING RETURNING event_id",
            (
                event["event_id"], event["event_type"], topic_for(event["event_type"], prefix),
                datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00")), Jsonb(event),
            ),
        ).fetchone()
        stored += row is not None
    conn.commit()
    return stored


def make_producer(settings: Settings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        compression_type="gzip",
    )


async def publish_pending(
    conn: psycopg.Connection, producer: AIOKafkaProducer, limit: int = 5000
) -> int:
    """Odošle neodoslané udalosti v poradí seq. Vráti počet odoslaných."""
    rows = conn.execute(
        "SELECT seq, topic, payload FROM core.event_log WHERE published_at IS NULL "
        "ORDER BY seq LIMIT %s",
        (limit,),
    ).fetchall()
    sent = 0
    for row in rows:
        event = row["payload"]
        try:
            await producer.send_and_wait(
                row["topic"],
                json.dumps(event, ensure_ascii=False).encode("utf-8"),
                key=key_for(event).encode("utf-8"),
                headers=[("event_type", event["event_type"].encode("utf-8"))],
            )
        except KafkaError as e:
            log.error("odoslanie do Kafky zlyhalo (seq %s): %s", row["seq"], e)
            break  # poradie sa nesmie prerušiť: zvyšok zostane na ďalší beh
        conn.execute(
            "UPDATE core.event_log SET published_at = %s WHERE seq = %s",
            (datetime.now(UTC), row["seq"]),
        )
        conn.commit()
        sent += 1
    return sent


async def _publish(conn: psycopg.Connection, settings: Settings) -> int:
    producer = make_producer(settings)
    try:
        await producer.start()
    except (KafkaError, OSError) as e:
        log.error("Kafka nie je dostupná, udalosti zostávajú v event_log: %s", e)
        await producer.stop()
        return 0
    try:
        return await publish_pending(conn, producer)
    finally:
        await producer.stop()


def publish_events(
    conn: psycopg.Connection, events: list[dict], settings: Settings | None = None
) -> tuple[int, int]:
    """Uloží udalosti a odošle všetko neodoslané. Vráti (uložené, odoslané)."""
    settings = settings or get_settings()
    stored = store_events(conn, events, settings.kafka_topic_prefix)
    sent = asyncio.run(_publish(conn, settings))
    return stored, sent
