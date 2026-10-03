from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal

import psycopg

from krakow_di.ids import make_offer_id


@dataclass(frozen=True)
class OfferObservation:
    """One scraped offer. Seats are owned by the generator and are not part of it."""

    source: str
    origin_iata: str
    destination_iata: str
    departure_at: datetime
    airline_iata: str
    price: Decimal
    currency: str
    observed_at: datetime
    arrival_at: datetime | None = None
    flight_number: str | None = None
    stops: int = 0

    @property
    def offer_id(self) -> str:
        return make_offer_id(
            self.source, self.origin_iata, self.destination_iata, self.departure_at,
            self.airline_iata,
        )


@dataclass(frozen=True)
class UpsertResult:
    offer_id: str
    change: Literal["found", "observed", "price_changed"]
    old_price: Decimal | None = None


def upsert_offer(conn: psycopg.Connection, obs: OfferObservation) -> UpsertResult:
    """Insert or refresh an offer and append a scrape row to flight_offer_history.

    Runs inside the caller's transaction (no commit here). seats_* are never touched.
    """
    offer_id = obs.offer_id
    existing = conn.execute(
        "SELECT price FROM core.flight_offer WHERE offer_id = %s FOR UPDATE", (offer_id,)
    ).fetchone()

    if existing is None:
        row = conn.execute(
            """
            INSERT INTO core.flight_offer (
                offer_id, source, origin_iata, destination_iata, departure_at, arrival_at,
                airline_iata, flight_number, stops, price, currency, first_seen_at, last_seen_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (offer_id) DO NOTHING
            RETURNING offer_id
            """,
            (
                offer_id, obs.source, obs.origin_iata.upper(), obs.destination_iata.upper(),
                obs.departure_at, obs.arrival_at, obs.airline_iata.upper(), obs.flight_number,
                obs.stops, obs.price, obs.currency.upper(), obs.observed_at, obs.observed_at,
            ),
        ).fetchone()
        if row is not None:
            change, old_price = "found", None
        else:  # lost a race with a concurrent insert
            return upsert_offer(conn, obs)
    else:
        old_price = existing["price"]
        change = "price_changed" if old_price != obs.price else "observed"
        conn.execute(
            """
            UPDATE core.flight_offer
               SET price = %s, currency = %s, arrival_at = COALESCE(%s, arrival_at),
                   flight_number = COALESCE(%s, flight_number), stops = %s,
                   last_seen_at = GREATEST(last_seen_at, %s)
             WHERE offer_id = %s
            """,
            (obs.price, obs.currency.upper(), obs.arrival_at, obs.flight_number, obs.stops,
             obs.observed_at, offer_id),
        )

    conn.execute(
        """
        INSERT INTO core.flight_offer_history
            (offer_id, observed_at, price, seats_left, status, cause)
        SELECT offer_id, %s, price, seats_left, status, 'scrape'
          FROM core.flight_offer WHERE offer_id = %s
        """,
        (obs.observed_at, offer_id),
    )
    return UpsertResult(offer_id, change, old_price if change == "price_changed" else None)
