"""Spoločná logika SSE aj WebSocket: dobehnutie z event_log (Last-Event-ID) a potom živý prúd."""

from collections.abc import AsyncIterator

from psycopg_pool import AsyncConnectionPool

from krakow_di.api.filters import matches, to_like
from krakow_di.api.hub import EventHub, Subscription

CATCH_UP_BATCH = 500


async def fetch_events(
    pool: AsyncConnectionPool, after_seq: int, patterns: list[str], limit: int
) -> list[dict]:
    sql = "SELECT seq, payload FROM core.event_log WHERE seq > %s"
    params: list = [after_seq]
    if patterns:
        sql += " AND event_type LIKE ANY(%s)"
        params.append(to_like(patterns))
    sql += " ORDER BY seq LIMIT %s"
    params.append(limit)
    async with pool.connection() as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()


async def event_feed(
    pool: AsyncConnectionPool,
    hub: EventHub,
    patterns: list[str],
    last_seq: int | None,
    sub: Subscription,
) -> AsyncIterator[tuple[int, dict]]:
    """Najprv udalosti s seq > last_seq z event_log, potom živé z hubu bez medzier a duplicít.

    Odber (`sub`) musí byť vytvorený ešte pred volaním: živé udalosti sa počas dobiehania
    ukladajú do jeho fronty a duplicity sa odfiltrujú podľa seq.
    """
    cursor = last_seq if last_seq is not None else 0
    if last_seq is not None:
        while True:
            rows = await fetch_events(pool, cursor, patterns, CATCH_UP_BATCH)
            for r in rows:
                cursor = r["seq"]
                yield r["seq"], r["payload"]
            if len(rows) < CATCH_UP_BATCH:
                break
    while True:
        item = await sub.queue.get()
        if item is None or sub.overflowed:
            return  # klient bol príliš pomalý; nech sa pripojí znova s Last-Event-ID
        seq, event = item
        if seq <= cursor or not matches(event["event_type"], patterns):
            continue
        cursor = seq
        yield seq, event
