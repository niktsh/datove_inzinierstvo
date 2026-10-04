"""Adaptér pre Server-Sent Events: pri výpadku sa pripojí znova s Last-Event-ID."""

import asyncio
import logging

import httpx

from krakow_di.collectors.http import USER_AGENT
from krakow_di.lake.adapters.base import Adapter, Sink
from krakow_di.lake.message import LakeMessage

log = logging.getLogger(__name__)


def parse_sse_lines(lines: list[str]) -> dict | None:
    """Jeden SSE rámec (riadky bez prázdneho riadka) -> {id, event, data} alebo None."""
    frame: dict = {}
    data: list[str] = []
    for line in lines:
        if line.startswith(":") or not line:
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "data":
            data.append(value)
        elif field in ("id", "event"):
            frame[field] = value
    if not data:
        return None
    frame["data"] = "\n".join(data)
    return frame


class SseAdapter(Adapter):
    channel = "sse"

    def __init__(self, team: str, url: str, headers: dict | None = None,
                 client: httpx.AsyncClient | None = None):
        self.team, self.url, self.headers = team, url, headers or {}
        self.client = client
        self.last_id: str | None = None

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        client = self.client or httpx.AsyncClient(timeout=None)
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream", **self.headers}
        if self.last_id:
            headers["Last-Event-ID"] = self.last_id
        async with client.stream("GET", self.url, headers=headers) as resp:
            resp.raise_for_status()
            buf: list[str] = []
            async for line in resp.aiter_lines():
                if stop.is_set():
                    return
                if line != "":
                    buf.append(line)
                    continue
                frame = parse_sse_lines(buf)
                buf = []
                if frame is None:
                    continue
                ref = {"url": self.url}
                if "id" in frame:
                    ref["id"] = frame["id"]
                if "event" in frame:
                    ref["event"] = frame["event"]
                await sink(LakeMessage(
                    team=self.team, channel=self.channel, content_type="text/event-stream",
                    source_ref=ref if "id" in frame else None,
                    payload_raw=frame["data"].encode("utf-8"),
                ))
                if "id" in frame:
                    self.last_id = frame["id"]
        raise ConnectionError(f"SSE spojenie {self.url} sa skončilo")
