"""Adaptér pre Kafku (aj naše vlastné udalosti): obyčajný consumer s vlastnou consumer group."""

import asyncio
import re
import ssl
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer

from krakow_di.lake.adapters.base import Adapter, Sink
from krakow_di.lake.message import LakeMessage


class KafkaAdapter(Adapter):
    channel = "kafka"

    def __init__(
        self,
        team: str,
        bootstrap: str,
        group_id: str,
        topics: list[str] | None = None,
        topics_regex: str | None = None,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str | None = None,
        username: str | None = None,
        password: str | None = None,
        offset_reset: str = "earliest",
    ):
        if not topics and not topics_regex:
            raise ValueError(f"{team}: treba zadať topics alebo topics_regex")
        self.team, self.bootstrap, self.group_id = team, bootstrap, group_id
        self.topics, self.topics_regex = topics or [], topics_regex
        self.auth: dict = {"security_protocol": security_protocol}
        if sasl_mechanism:
            self.auth.update(sasl_mechanism=sasl_mechanism, sasl_plain_username=username,
                             sasl_plain_password=password)
        if "SSL" in security_protocol:
            self.auth["ssl_context"] = ssl.create_default_context()
        self.offset_reset = offset_reset

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap, group_id=self.group_id,
            enable_auto_commit=False, auto_offset_reset=self.offset_reset, **self.auth,
        )
        await consumer.start()
        try:
            if self.topics_regex:
                consumer.subscribe(pattern=re.compile(self.topics_regex))
            else:
                consumer.subscribe(self.topics)
            while not stop.is_set():
                batch = await consumer.getmany(timeout_ms=1000, max_records=200)
                if not batch:
                    continue
                for messages in batch.values():
                    for m in messages:
                        headers = {k: v for k, v in (m.headers or [])}
                        ctype = headers.get("content-type", b"").decode() or None
                        await sink(LakeMessage(
                            team=self.team, channel=self.channel,
                            source_ref={"topic": m.topic, "partition": m.partition,
                                        "offset": m.offset},
                            content_type=ctype,
                            payload_raw=m.value if m.value is not None else b"",
                            received_at=datetime.now(UTC),
                        ))
                await consumer.commit()  # až po zápise celej dávky do DB
        finally:
            await consumer.stop()
