"""Spoločné rozhranie adaptéra: pripojí sa k cudziemu zdroju (ich protokolom) a odovzdá správy."""

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from krakow_di.lake.message import LakeMessage

# Sink uloží správu do DB a vráti sa až po commite. Adaptér smie potvrdiť offset zdroja
# (napr. Kafka commit) až po návrate zo sinku: at-least-once, duplicity odstráni source_ref.
Sink = Callable[[LakeMessage], Awaitable[None]]


class Adapter(ABC):
    team: str
    channel: str

    @abstractmethod
    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        """Beží, kým nie je `stop` nastavený. Chyby spojenia sa vyhadzujú: runner adaptér
        reštartuje s backoffom."""

    @property
    def name(self) -> str:
        return f"{self.team}/{self.channel}"
