import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal

import psycopg


@dataclass(frozen=True)
class Performance:
    performance_id: str
    title: str
    stage: str | None
    starts_at: datetime
    url: str | None
    status: str = "on_sale"
    instance_id: int | None = None


@dataclass(frozen=True)
class CategorySeats:
    """Jedna cenová kategória jedného predstavenia: kľúč je (názov, cena)."""

    name: str
    price: Decimal
    seats_available: int


@dataclass(frozen=True)
class Sale:
    sale_id: uuid.UUID
    performance_id: str
    category: str
    quantity: int  # záporné = miesta vrátené
    unit_price: Decimal
    currency: str
    unit_price_eur: Decimal | None
    fx_rate: Decimal | None
    detected_from: datetime
    detected_to: datetime


def upsert_performance(
    conn: psycopg.Connection, perf: Performance, observed_at: datetime
) -> tuple[Literal["found", "updated", "unchanged"], str]:
    """Vloží alebo obnoví predstavenie. Vráti (druh, skutočne uložené performance_id).

    Predstavenie sa hľadá najprv podľa instance id divadla (to prežije prechod medzi
    „vstupenky v predaji“ a „vypredané“, keď odkaz pokladne aj slug zmiznú).
    Chýbajúce url nikdy neprepíše známe url.
    """
    row = None
    if perf.instance_id is not None:
        row = conn.execute(
            "SELECT * FROM core.theater_performance WHERE instance_id = %s FOR UPDATE",
            (perf.instance_id,),
        ).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT * FROM core.theater_performance WHERE performance_id = %s FOR UPDATE",
            (perf.performance_id,),
        ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO core.theater_performance (performance_id, title, stage, starts_at, url, "
            "status, instance_id, first_seen_at, last_seen_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (perf.performance_id, perf.title, perf.stage, perf.starts_at, perf.url, perf.status,
             perf.instance_id, observed_at, observed_at),
        )
        return "found", perf.performance_id
    url = perf.url or row["url"]
    status = row["status"]
    if perf.status == "sold_out":
        status = "sold_out"  # program to tvrdí; snímky vrátia on_sale, ak sa miesta objavia
    changed = (row["title"], row["stage"], row["starts_at"], row["url"], row["status"]) != (
        perf.title, perf.stage, perf.starts_at, url, status,
    )
    conn.execute(
        "UPDATE core.theater_performance SET title=%s, stage=%s, starts_at=%s, url=%s, status=%s, "
        "instance_id=COALESCE(instance_id, %s), last_seen_at=%s WHERE performance_id=%s",
        (perf.title, perf.stage, perf.starts_at, url, status, perf.instance_id, observed_at,
         row["performance_id"]),
    )
    return ("updated" if changed else "unchanged"), row["performance_id"]


def set_status(conn: psycopg.Connection, performance_id: str, status: str) -> bool:
    """Vráti True, ak sa stav skutočne zmenil."""
    row = conn.execute(
        "UPDATE core.theater_performance SET status = %s WHERE performance_id = %s "
        "AND status <> %s RETURNING performance_id",
        (status, performance_id, status),
    ).fetchone()
    return row is not None


def set_repertoire(
    conn: psycopg.Connection, performance_id: str, repertoire_id: int, location: str | None
) -> None:
    conn.execute(
        "UPDATE core.theater_performance SET repertoire_id=%s, location=%s WHERE performance_id=%s",
        (repertoire_id, location, performance_id),
    )


def mark_past(conn: psycopg.Connection, now: datetime) -> int:
    cur = conn.execute(
        "UPDATE core.theater_performance SET status='past' "
        "WHERE starts_at <= %s AND status <> 'past'",
        (now,),
    )
    return cur.rowcount


def performances_to_snapshot(conn: psycopg.Connection, now: datetime) -> list[dict]:
    return conn.execute(
        "SELECT performance_id, repertoire_id, url, status, starts_at "
        "FROM core.theater_performance "
        "WHERE starts_at > %s AND status <> 'cancelled' AND url IS NOT NULL "
        "ORDER BY starts_at, performance_id",
        (now,),
    ).fetchall()


def performances_without_repertoire(conn: psycopg.Connection, now: datetime) -> list[dict]:
    return conn.execute(
        "SELECT performance_id, url FROM core.theater_performance "
        "WHERE repertoire_id IS NULL AND url IS NOT NULL AND starts_at > %s "
        "ORDER BY starts_at, performance_id",
        (now,),
    ).fetchall()


def previous_snapshot(
    conn: psycopg.Connection, performance_id: str
) -> tuple[datetime, dict[tuple[str, Decimal], int]] | None:
    """Kategórie najnovšej snímky, kľúč je (názov, cena)."""
    rows = conn.execute(
        "SELECT observed_at, category, price, seats_available FROM core.theater_snapshot "
        "WHERE performance_id = %s AND observed_at = "
        "(SELECT max(observed_at) FROM core.theater_snapshot WHERE performance_id = %s)",
        (performance_id, performance_id),
    ).fetchall()
    if not rows:
        return None
    return rows[0]["observed_at"], {
        (r["category"], r["price"]): r["seats_available"] for r in rows
    }


def insert_snapshot(
    conn: psycopg.Connection,
    performance_id: str,
    observed_at: datetime,
    categories: list[CategorySeats],
    currency: str,
    fx_rate: Decimal | None,
    eur_prices: dict[tuple[str, Decimal], Decimal | None],
) -> None:
    for c in categories:
        conn.execute(
            "INSERT INTO core.theater_snapshot (performance_id, observed_at, category, price, "
            "currency, seats_available, price_eur, fx_rate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (performance_id, observed_at, c.name, c.price, currency, c.seats_available,
             eur_prices.get((c.name, c.price)), fx_rate),
        )


def insert_sale(conn: psycopg.Connection, s: Sale) -> None:
    conn.execute(
        "INSERT INTO core.theater_sale (sale_id, performance_id, category, quantity, unit_price, "
        "currency, unit_price_eur, fx_rate, detected_from, detected_to) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (s.sale_id, s.performance_id, s.category, s.quantity, s.unit_price, s.currency,
         s.unit_price_eur, s.fx_rate, s.detected_from, s.detected_to),
    )
