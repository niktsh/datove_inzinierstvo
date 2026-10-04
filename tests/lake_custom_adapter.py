"""Vzorový vlastný adaptér tímu pre test `type: custom`."""

import asyncio

from krakow_di.lake.adapters.base import Adapter, Sink
from krakow_di.lake.message import LakeMessage


class DemoAdapter(Adapter):
    channel = "demo"

    def __init__(self, team, username=None, password=None, headers=None, **config):
        self.team, self.username, self.password = team, username, password
        self.headers, self.config = headers, config

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        await sink(LakeMessage(team=self.team, channel=self.channel, payload_raw=b"{}",
                               source_ref={"n": 1}))
