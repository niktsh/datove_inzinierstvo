"""Generátor predaja leteniek: v tikoch simuluje kapacitu, predaje, dynamickú cenu a vypršanie.

Generátor sa dotýka iba `core.flight_offer` (seats_*, status, price_markup), `core.ticket_sale`
a riadkov histórie s `cause='generator'`; surové dáta (`raw.*`) nikdy. Čas aj seed sú
vstupy (determinizmus pre testy): rovnaký seed + rovnaký stav DB + rovnaké časy = rovnaké udalosti.
"""

import logging
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

import psycopg

from krakow_di.events.envelope import iso_local, iso_utc, make_event
from krakow_di.generator import model
from krakow_di.generator.model import TICK_REFERENCE_SECONDS, ModelParams

log = logging.getLogger(__name__)
CENT = Decimal("0.01")


def bump(markup: Decimal, rng: random.Random) -> Decimal:
    """Prirážka po prekročení prahu obsadenosti (+3–12 %)."""
    return (markup * Decimal(str(model.markup_step(rng)))).quantize(Decimal("0.00001"))


def effective_price(base: Decimal, markup: Decimal) -> Decimal:
    return (base * markup).quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass
class TickResult:
    events: list[dict] = field(default_factory=list)
    initialized: int = 0
    sales: int = 0
    seats_sold: int = 0
    price_changes: int = 0
    sold_out: int = 0
    expired: int = 0

    def merge(self, other: "TickResult") -> None:
        self.events.extend(other.events)
        for name in ("initialized", "sales", "seats_sold", "price_changes", "sold_out", "expired"):
            setattr(self, name, getattr(self, name) + getattr(other, name))


class Generator:
    def __init__(self, params: ModelParams, seed: int | None, producer: str):
        self.params = params
        self.seed = seed if seed is not None else random.SystemRandom().getrandbits(48)
        self.producer = producer

    # ---- náhodnosť (odvodená zo seedu, nezávislá od poradia spracovania) --------------------
    def _rng(self, *parts: object) -> random.Random:
        return random.Random("|".join([str(self.seed), *map(str, parts)]))

    def popularity(self, offer_id: str) -> float:
        return model.popularity(self._rng("pop", offer_id), self.params.popularity_sigma)

    # ---- jeden tik --------------------------------------------------------------------------
    def tick(self, conn: psycopg.Connection, now: datetime, dt_seconds: float = 300) -> TickResult:
        result = TickResult()
        self._expire(conn, now, result)
        self._initialize(conn, now, result)
        self._sell(conn, now, dt_seconds, result)
        conn.commit()
        return result

    def run_interval(
        self, conn: psycopg.Connection, start: datetime, seconds: float
    ) -> TickResult:
        """Simuluje `seconds` sekúnd po 5-minútových tikoch (pri zrýchlení sa dlhý úsek delí)."""
        total = TickResult()
        steps = max(1, round(seconds / TICK_REFERENCE_SECONDS))
        dt = seconds / steps
        for i in range(steps):
            total.merge(self.tick(conn, start + timedelta(seconds=dt * (i + 1)), dt))
        return total

    # ---- kroky ------------------------------------------------------------------------------
    def _expire(self, conn: psycopg.Connection, now: datetime, result: TickResult) -> None:
        rows = conn.execute(
            "UPDATE core.flight_offer SET status = 'expired' "
            "WHERE departure_at <= %s AND status <> 'expired' "
            "RETURNING offer_id, source, price, price_markup, seats_left",
            (now,),
        ).fetchall()
        for r in sorted(rows, key=lambda r: r["offer_id"]):
            self._history(conn, r["offer_id"], now, effective_price(r["price"], r["price_markup"]),
                          r["seats_left"], "expired")
            result.events.append(make_event(
                "flight.offer.expired", "generator",
                {"offer_id": r["offer_id"], "reason": "departed"}, now, self.producer))
            result.expired += 1

    def _initialize(self, conn: psycopg.Connection, now: datetime, result: TickResult) -> None:
        rows = conn.execute(
            "SELECT offer_id, price, departure_at FROM core.flight_offer "
            "WHERE status = 'active' AND seats_total IS NULL AND departure_at > %s "
            "ORDER BY offer_id",
            (now,),
        ).fetchall()
        for r in rows:
            rng = self._rng("init", r["offer_id"])
            days = (r["departure_at"] - now).total_seconds() / 86400
            total = model.initial_capacity(rng)
            fill = model.initial_fill(rng, self.popularity(r["offer_id"]), days)
            left = max(1, total - round(total * fill))
            passed = model.crossed_thresholds(0.0, (total - left) / total)
            markup = Decimal(1)
            for _ in range(passed):  # počiatočné prirážky bez udalostí (stav pred prvým výskytom)
                markup = bump(markup, rng)
            conn.execute(
                "UPDATE core.flight_offer SET seats_total=%s, seats_left=%s, price_markup=%s "
                "WHERE offer_id=%s",
                (total, left, markup, r["offer_id"]),
            )
            self._history(conn, r["offer_id"], now, effective_price(r["price"], markup), left,
                          "active")
            result.initialized += 1

    def _sell(
        self, conn: psycopg.Connection, now: datetime, dt_seconds: float, result: TickResult
    ) -> None:
        rows = conn.execute(
            "SELECT o.*, m.median FROM core.flight_offer o "
            "JOIN (SELECT origin_iata, destination_iata, "
            "             percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median "
            "      FROM core.flight_offer WHERE status = 'active' "
            "      GROUP BY origin_iata, destination_iata) m "
            "USING (origin_iata, destination_iata) "
            "WHERE o.status = 'active' AND o.seats_left > 0 AND o.departure_at > %s "
            "ORDER BY o.offer_id",
            (now,),
        ).fetchall()
        rng = self._rng("tick", iso_utc(now), round(dt_seconds))
        for o in rows:
            days = (o["departure_at"] - now).total_seconds() / 86400
            p = model.sale_probability(
                self.params, self.popularity(o["offer_id"]), days,
                float(o["price"]), float(o["median"]), dt_seconds,
            )
            if rng.random() >= p:
                continue
            self._sale(conn, o, now, rng, result)

    def _sale(
        self, conn: psycopg.Connection, o: dict, now: datetime, rng: random.Random,
        result: TickResult,
    ) -> None:
        qty = model.sale_size(rng, o["seats_left"])
        unit = effective_price(o["price"], o["price_markup"])
        total, before = o["seats_total"], o["seats_left"]
        after = before - qty
        sale_id = uuid.UUID(int=rng.getrandbits(128), version=4)
        conn.execute(
            "INSERT INTO core.ticket_sale (sale_id, offer_id, quantity, unit_price, total_price, "
            "currency, sold_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (sale_id, o["offer_id"], qty, unit, unit * qty, o["currency"], now),
        )
        result.sales += 1
        result.seats_sold += qty
        result.events.append(make_event(
            "flight.ticket.sold", "generator",
            {"sale_id": str(sale_id), "offer_id": o["offer_id"],
             "origin_iata": o["origin_iata"], "destination_iata": o["destination_iata"],
             "departure_at": iso_local(o["departure_at"]), "airline_iata": o["airline_iata"],
             "flight_number": o["flight_number"], "quantity": qty, "unit_price": float(unit),
             "total_price": float(unit * qty), "currency": o["currency"],
             "seats_left_after": after, "sold_at": iso_utc(now)},
            now, self.producer, event_id=uuid.UUID(int=rng.getrandbits(128), version=4)))

        markup = o["price_markup"]
        crossed = model.crossed_thresholds((total - before) / total, (total - after) / total)
        for _ in range(crossed):
            markup = bump(markup, rng)
        status = "sold_out" if after == 0 else "active"
        conn.execute(
            "UPDATE core.flight_offer SET seats_left=%s, status=%s, price_markup=%s "
            "WHERE offer_id=%s",
            (after, status, markup, o["offer_id"]),
        )
        self._history(conn, o["offer_id"], now, effective_price(o["price"], markup), after, status)
        if markup != o["price_markup"]:
            result.price_changes += 1
            result.events.append(make_event(
                "flight.offer.price_changed", "generator",
                {"offer_id": o["offer_id"], "old_price": float(unit),
                 "new_price": float(effective_price(o["price"], markup)),
                 "currency": o["currency"], "reason": "load_factor"},
                now, self.producer, event_id=uuid.UUID(int=rng.getrandbits(128), version=4)))
        if after == 0:
            result.sold_out += 1
            result.events.append(make_event(
                "flight.offer.sold_out", "generator",
                {"offer_id": o["offer_id"], "last_price": float(unit), "currency": o["currency"]},
                now, self.producer, event_id=uuid.UUID(int=rng.getrandbits(128), version=4)))

    @staticmethod
    def _history(
        conn: psycopg.Connection, offer_id: str, at: datetime, price: Decimal,
        seats_left: int | None, status: str,
    ) -> None:
        conn.execute(
            "INSERT INTO core.flight_offer_history (offer_id, observed_at, price, seats_left, "
            "status, cause) VALUES (%s,%s,%s,%s,%s,'generator')",
            (offer_id, at, price, seats_left, status),
        )
