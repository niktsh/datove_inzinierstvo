"""Adaptéry tímu 7 (letenky SSE, divadlo REST); popis: docs/timy/team_7.md.

Ich rozhranie nemá `id` udalostí ani históriu zmien: pri každom pripojení pošle znova snímky
tabuliek a spojenie sa po ~45 s zatvorí. Správy preto ukladáme tak, ako prišli (surové bajty),
a duplicity odstraňujeme podľa odtlačku obsahu (`sha256` v `source_ref`): rovnaký snímok sa
uloží raz, zmenený snímok ako nová správa. Žiadna normalizácia.
"""

import asyncio
import hashlib
import logging
import time
from datetime import UTC, datetime

import httpx

from krakow_di.collectors.http import USER_AGENT
from krakow_di.lake.adapters.base import Adapter, Sink
from krakow_di.lake.adapters.sse import parse_sse_lines
from krakow_di.lake.message import LakeMessage

log = logging.getLogger(__name__)

SKIPPED_EVENTS = {"heartbeat"}  # iba udržiavanie spojenia (čas každých ~10 s), nie dáta


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class Team7SseAdapter(Adapter):
    """Letenky: SSE `/api/stream` (udalosti ready, snapshot, change, warning, heartbeat)."""

    channel = "sse"

    def __init__(self, team: str, url: str, reconnect_seconds: float = 2.0, headers=None,
                 client: httpx.AsyncClient | None = None, max_connections: int | None = None,
                 min_healthy_seconds: float = 10.0, max_pause_seconds: float = 60.0,
                 **_ignored):
        self.team, self.url = team, url
        self.reconnect_seconds, self.headers = reconnect_seconds, headers or {}
        self.client, self.max_connections = client, max_connections
        # spojenie kratšie než min_healthy_seconds = server nás odpája hneď: pauza sa zdvojnásobuje
        self.min_healthy_seconds, self.max_pause_seconds = min_healthy_seconds, max_pause_seconds
        self.pause = reconnect_seconds

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        client = self.client or httpx.AsyncClient(timeout=None)
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream", **self.headers}
        connections = 0
        while not stop.is_set():
            started = time.monotonic()
            # chyba spojenia alebo HTTP stav != 2xx sa vyhodí: runner reštartuje s backoffom
            frames, last_event = await self._connection(client, headers, sink, stop)
            lasted = time.monotonic() - started
            log.info("%s: spojenie trvalo %.1f s, rámcov %d, posledná udalosť %s",
                     self.name, lasted, frames, last_event)
            connections += 1
            if lasted >= self.min_healthy_seconds:
                self.pause = self.reconnect_seconds
            else:
                self.pause = min(self.pause * 2, self.max_pause_seconds)
            if self.max_connections is not None and connections >= self.max_connections:
                return
            try:  # server spojenie zatvára bežne (~45 s): krátka pauza a znova
                await asyncio.wait_for(stop.wait(), timeout=self.pause)
            except TimeoutError:
                pass

    async def _connection(
        self, client, headers, sink: Sink, stop: asyncio.Event
    ) -> tuple[int, str | None]:
        """Jedno spojenie: vráti počet rámcov a názov poslednej udalosti (do logu)."""
        frames, last_event = 0, None
        async with client.stream("GET", self.url, headers=headers) as resp:
            resp.raise_for_status()
            buf: list[str] = []
            async for line in resp.aiter_lines():
                if stop.is_set():
                    return frames, last_event
                if line != "":
                    buf.append(line)
                    continue
                frame = parse_sse_lines(buf)
                buf = []
                if frame is None:
                    continue
                frames += 1
                last_event = frame.get("event", "message")
                if last_event in SKIPPED_EVENTS:
                    continue
                payload = frame["data"].encode("utf-8")
                await sink(LakeMessage(
                    team=self.team, channel=self.channel, content_type="text/event-stream",
                    source_ref={"url": self.url, "event": frame.get("event", "message"),
                                "sha256": digest(payload)},
                    payload_raw=payload,
                ))
        return frames, last_event


class Team7RestAdapter(Adapter):
    """Divadlo: REST `/api/theatre` (JSON `{success, count, data[]}`, stránkovanie `offset`)."""

    channel = "rest"

    def __init__(self, team: str, url: str, interval_seconds: float = 3600,
                 page_size: int = 1000, max_pages: int = 20, headers=None,
                 client: httpx.AsyncClient | None = None, **_ignored):
        self.team, self.url, self.interval = team, url, interval_seconds
        self.page_size, self.max_pages = page_size, max_pages
        self.headers, self.client = headers or {}, client

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        client = self.client or httpx.AsyncClient(timeout=60)
        headers = {"User-Agent": USER_AGENT, **self.headers}
        while not stop.is_set():
            await self._poll(client, headers, sink)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.interval)
            except TimeoutError:
                pass

    async def _poll(self, client, headers, sink: Sink) -> None:
        """Stránkuje, kým nepríde neplná stránka. Každá stránka sa uloží ako celá odpoveď."""
        for page in range(self.max_pages):
            offset = page * self.page_size
            resp = await client.get(
                self.url, params={"limit": self.page_size, "offset": offset}, headers=headers)
            if resp.status_code // 100 != 2:
                log.warning("%s: %s vrátil HTTP %s, stránka sa neukladá",
                            self.name, resp.url, resp.status_code)
                return
            rows = _row_count(resp)
            if rows == 0 and page > 0:
                return  # prázdna stránka za koncom nie je dátum
            await sink(LakeMessage(
                team=self.team, channel=self.channel,
                content_type=resp.headers.get("content-type"),
                source_ref={"url": self.url, "offset": offset, "sha256": digest(resp.content)},
                payload_raw=resp.content, received_at=datetime.now(UTC),
            ))
            if rows < self.page_size:
                return


def _row_count(resp: httpx.Response) -> int:
    try:
        return len(resp.json()["data"])
    except (ValueError, KeyError, TypeError):
        return 0
