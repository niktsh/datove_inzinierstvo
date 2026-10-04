"""Rozdeľovač živých udalostí k pripojeným klientom (SSE/WebSocket)."""

import asyncio
from dataclasses import dataclass, field

Item = tuple[int, dict]  # (seq z core.event_log, celá udalosť)


@dataclass(eq=False)  # musí byť hashovateľné (ukladá sa do množiny)
class Subscription:
    queue: asyncio.Queue
    overflowed: bool = False


@dataclass
class EventHub:
    """Každý klient má vlastnú frontu; príliš pomalý sa odpojí a dobehne cez Last-Event-ID."""

    queue_size: int = 1000
    _subs: set[Subscription] = field(default_factory=set)
    _loop: asyncio.AbstractEventLoop | None = None

    def subscribe(self) -> Subscription:
        self._loop = asyncio.get_running_loop()
        sub = Subscription(asyncio.Queue(self.queue_size))
        self._subs.add(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        self._subs.discard(sub)

    @property
    def subscribers(self) -> int:
        return len(self._subs)

    def publish(self, item: Item) -> None:
        for sub in list(self._subs):
            try:
                sub.queue.put_nowait(item)
            except asyncio.QueueFull:
                sub.overflowed = True
                self._subs.discard(sub)
                # prebudí čakajúceho klienta, aby zistil, že bol odpojený
                try:
                    sub.queue.get_nowait()
                    sub.queue.put_nowait(None)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass

    def publish_threadsafe(self, item: Item) -> None:
        """Pre volanie z iného vlákna (napr. z testov)."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self.publish, item)
