"""Adaptér pre WebSocket: každá správa sa uloží tak, ako prišla (bez stabilného id)."""

import asyncio

import websockets

from krakow_di.lake.adapters.base import Adapter, Sink
from krakow_di.lake.message import LakeMessage


class WebSocketAdapter(Adapter):
    channel = "ws"

    def __init__(self, team: str, url: str, headers: dict | None = None):
        self.team, self.url, self.headers = team, url, headers or {}

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        async with websockets.connect(self.url, additional_headers=self.headers or None) as ws:
            while not stop.is_set():
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except TimeoutError:
                    continue
                is_text = isinstance(message, str)
                await sink(LakeMessage(
                    team=self.team, channel=self.channel,
                    content_type="text/plain" if is_text else "application/octet-stream",
                    source_ref=None,  # WebSocket nemá stabilné id: duplicity sa neodstraňujú
                    payload_raw=message.encode("utf-8") if is_text else bytes(message),
                ))
