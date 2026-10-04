"""Runner: beží všetky adaptéry súbežne a spadnutý adaptér reštartuje s backoffom."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg

from krakow_di.lake.adapters.base import Adapter
from krakow_di.lake.message import LakeMessage
from krakow_di.lake.writer import write_message

log = logging.getLogger(__name__)
BACKOFF_START, BACKOFF_MAX = 1.0, 60.0


@dataclass
class AdapterStats:
    written: int = 0
    duplicates: int = 0
    restarts: int = 0
    last_error: str | None = None
    last_message_at: datetime | None = None


async def run_adapter(
    adapter: Adapter, database_url: str, stop: asyncio.Event, stats: AdapterStats,
    backoff_start: float = BACKOFF_START, backoff_max: float = BACKOFF_MAX,
) -> None:
    delay = backoff_start
    while not stop.is_set():
        try:
            async with await psycopg.AsyncConnection.connect(database_url) as conn:
                async def sink(msg: LakeMessage) -> None:
                    nonlocal delay
                    if await write_message(conn, msg):
                        stats.written += 1
                    else:
                        stats.duplicates += 1
                    stats.last_message_at = datetime.now(UTC)
                    delay = backoff_start  # adaptér funguje: backoff sa vynuluje

                await adapter.run(sink, stop)
            if stop.is_set():
                return
            log.warning("%s: adaptér skončil, reštartujem", adapter.name)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 (adaptér cudzieho zdroja nesmie zhodiť ostatné)
            stats.last_error = f"{type(e).__name__}: {e}"
            log.error("%s: %s, reštart o %.0f s", adapter.name, stats.last_error, delay)
        stats.restarts += 1
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            pass
        delay = min(delay * 2, backoff_max)


async def run_all(
    adapters: list[Adapter], database_url: str, stop: asyncio.Event,
    backoff_start: float = BACKOFF_START, backoff_max: float = BACKOFF_MAX,
) -> dict[str, AdapterStats]:
    stats = {a.name: AdapterStats() for a in adapters}
    await asyncio.gather(*(
        run_adapter(a, database_url, stop, stats[a.name], backoff_start, backoff_max)
        for a in adapters
    ))
    return stats
