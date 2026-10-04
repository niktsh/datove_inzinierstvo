"""Brána Kafka -> hub: číta topiky ako consumer group `api-gateway` a rozdáva klientom."""

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from psycopg_pool import AsyncConnectionPool

from krakow_di.api.hub import EventHub
from krakow_di.config import Settings
from krakow_di.events.routing import topic_names

log = logging.getLogger(__name__)
GROUP = "api-gateway"


class KafkaGateway:
    def __init__(self, settings: Settings, pool: AsyncConnectionPool, hub: EventHub):
        self.settings, self.pool, self.hub = settings, pool, hub
        self.connected = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="kafka-gateway")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _seq(self, event_id: str) -> int | None:
        # udalosť sa najprv zapíše do event_log a až potom pošle do Kafky; pri súbehu chvíľu počkáme
        for _ in range(10):
            async with self.pool.connection() as conn:
                cur = await conn.execute(
                    "SELECT seq FROM core.event_log WHERE event_id = %s", (event_id,)
                )
                row = await cur.fetchone()
            if row:
                return row["seq"]
            await asyncio.sleep(0.2)
        return None

    async def _run(self) -> None:
        topics = list(topic_names(self.settings.kafka_topic_prefix))
        while True:
            consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=self.settings.kafka_bootstrap_servers,
                group_id=GROUP,
                auto_offset_reset="latest",
            )
            try:
                await consumer.start()
                self.connected = True
                log.info("brána Kafky beží (group %s)", GROUP)
                async for msg in consumer:
                    try:
                        event = json.loads(msg.value)
                    except ValueError:
                        continue
                    seq = await self._seq(event.get("event_id", ""))
                    if seq is not None:
                        self.hub.publish((seq, event))
            except asyncio.CancelledError:
                raise
            except (KafkaError, OSError) as e:
                log.error("brána Kafky: %s, nový pokus o 5 s", e)
            finally:
                self.connected = False
                await consumer.stop()
            await asyncio.sleep(5)
