"""Adaptér pre REST/DB zdroje: pravidelne dopytuje URL a ukladá snímky odpovedí."""

import asyncio
import logging
from datetime import UTC, datetime

import httpx

from krakow_di.collectors.http import USER_AGENT
from krakow_di.lake.adapters.base import Adapter, Sink
from krakow_di.lake.message import LakeMessage

log = logging.getLogger(__name__)


class RestPollAdapter(Adapter):
    channel = "rest"

    def __init__(self, team: str, urls: list[str], interval_seconds: float = 300,
                 headers: dict | None = None, client: httpx.AsyncClient | None = None):
        self.team, self.urls, self.interval = team, urls, interval_seconds
        self.headers, self.client = headers or {}, client

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        client = self.client or httpx.AsyncClient(timeout=30)
        headers = {"User-Agent": USER_AGENT, **self.headers}
        while not stop.is_set():
            for url in self.urls:
                resp = await client.get(url, headers=headers)
                if resp.status_code // 100 != 2:
                    log.warning("%s: %s vrátil HTTP %s, snímka sa neukladá",
                                self.name, url, resp.status_code)
                    continue
                fetched = datetime.now(UTC)
                await sink(LakeMessage(
                    team=self.team, channel=self.channel,
                    content_type=resp.headers.get("content-type"),
                    source_ref={"url": url, "fetched_at": fetched.isoformat()},
                    payload_raw=resp.content, received_at=fetched,
                ))
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.interval)
            except TimeoutError:
                pass
