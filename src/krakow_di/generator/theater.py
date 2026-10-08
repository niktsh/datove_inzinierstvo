"""Generátor predaja vstupeniek do divadla: simulovaná vrstva nad reálnymi snímkami dostupnosti.

Generátor zapisuje iba do `core.theater_sale` (so `source='generator'`); snímky ani surové
dáta sa nedotýka, takže detekcia reálnych predajov z rozdielu snímok ostáva nezmenená.
Dostupnosť kategórie = miesta z najnovšej reálnej snímky mínus simulované predaje od tejto
snímky (po ďalšej reálnej snímke sa počítanie začína odznova). Čas aj seed sú vstupy
(determinizmus pre testy), ako pri generátore leteniek.
"""

import logging
import random
import uuid
from datetime import datetime, timedelta

import psycopg

from krakow_di.events.builders import theater_sale_data
from krakow_di.events.envelope import make_event
from krakow_di.generator import model
from krakow_di.generator.engine import TickResult
from krakow_di.generator.model import TICK_REFERENCE_SECONDS, TheaterParams
from krakow_di.repo import theater as repo

log = logging.getLogger(__name__)

AVAILABILITY_SQL = """
SELECT s.performance_id, s.category, s.price, s.currency, s.price_eur, s.fx_rate,
       p.starts_at, l.last_at,
       s.seats_available - COALESCE((
           SELECT sum(x.quantity) FROM core.theater_sale x
           WHERE x.performance_id = s.performance_id AND x.category = s.category
             AND x.unit_price = s.price AND x.source = 'generator'
             AND x.detected_to > l.last_at), 0) AS seats_left
FROM core.theater_snapshot s
JOIN (SELECT performance_id, max(observed_at) AS last_at
      FROM core.theater_snapshot GROUP BY performance_id) l
  ON l.performance_id = s.performance_id AND s.observed_at = l.last_at
JOIN core.theater_performance p ON p.performance_id = s.performance_id
WHERE p.starts_at > %s AND p.status = 'on_sale' AND p.url IS NOT NULL
ORDER BY s.performance_id, s.category, s.price
"""


class TheaterGenerator:
    def __init__(self, params: TheaterParams, seed: int | None, producer: str):
        self.params = params
        self.seed = seed if seed is not None else random.SystemRandom().getrandbits(48)
        self.producer = producer

    def _rng(self, *parts: object) -> random.Random:
        return random.Random("|".join([str(self.seed), "theater", *map(str, parts)]))

    def _uuid(self, kind: str, *parts: object) -> uuid.UUID:
        key = ":".join(["krakow-di", str(self.seed), "theater", kind, *map(str, parts)])
        return uuid.uuid5(uuid.NAMESPACE_URL, key)

    def popularity(self, performance_id: str) -> float:
        return model.popularity(self._rng("pop", performance_id), self.params.popularity_sigma)

    def tick(self, conn: psycopg.Connection, now: datetime, dt_seconds: float = 300) -> TickResult:
        result = TickResult()
        by_perf: dict[str, list[dict]] = {}
        for r in conn.execute(AVAILABILITY_SQL, (now,)).fetchall():
            by_perf.setdefault(r["performance_id"], []).append(r)
        rng = self._rng("tick", now.isoformat(), round(dt_seconds))
        for pid, cats in by_perf.items():
            free = [c for c in cats if c["seats_left"] > 0]
            if not free:
                continue
            days = (free[0]["starts_at"] - now).total_seconds() / 86400
            p = model.theater_sale_probability(
                self.params, self.popularity(pid), days, dt_seconds)
            if rng.random() >= p:
                continue
            # kategória sa vyberá úmerne počtu voľných miest
            cat = rng.choices(free, weights=[c["seats_left"] for c in free])[0]
            self._sale(conn, cat, now, dt_seconds, rng, result)
        conn.commit()
        return result

    def run_interval(
        self, conn: psycopg.Connection, start: datetime, seconds: float
    ) -> TickResult:
        total = TickResult()
        steps = max(1, round(seconds / TICK_REFERENCE_SECONDS))
        dt = seconds / steps
        for i in range(steps):
            total.merge(self.tick(conn, start + timedelta(seconds=dt * (i + 1)), dt))
        return total

    def _sale(
        self, conn: psycopg.Connection, c: dict, now: datetime, dt_seconds: float,
        rng: random.Random, result: TickResult,
    ) -> None:
        qty = model.theater_sale_size(rng, c["seats_left"])
        key = (c["performance_id"], c["category"], c["price"], c["seats_left"], now.isoformat())
        sale = repo.Sale(
            sale_id=self._uuid("sale", *key), performance_id=c["performance_id"],
            category=c["category"], quantity=qty, unit_price=c["price"], currency=c["currency"],
            unit_price_eur=c["price_eur"], fx_rate=c["fx_rate"],
            detected_from=now - timedelta(seconds=dt_seconds), detected_to=now,
            source="generator",
        )
        repo.insert_sale(conn, sale)
        result.sales += 1
        result.seats_sold += qty
        result.events.append(make_event(
            "theater.tickets.sold", "generator", theater_sale_data(sale), now, self.producer,
            event_id=self._uuid("event", *key)))

