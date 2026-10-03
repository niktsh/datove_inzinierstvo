from datetime import UTC, datetime, timedelta
from decimal import Decimal

from krakow_di.repo.flight_offers import OfferObservation, upsert_offer

T0 = datetime(2026, 10, 14, 6, 0, tzinfo=UTC)
DEP = datetime(2026, 11, 20, 5, 15, tzinfo=UTC)


def obs(price="54.99", at=T0, **kw):
    return OfferObservation(
        source="travelpayouts", origin_iata="BCN", destination_iata="KRK", departure_at=DEP,
        airline_iata="W6", price=Decimal(price), currency="EUR", observed_at=at, **kw,
    )


def history(conn, offer_id):
    return conn.execute(
        "SELECT price, cause FROM core.flight_offer_history WHERE offer_id=%s ORDER BY id",
        (offer_id,),
    ).fetchall()


def test_new_offer_is_found(conn):
    r = upsert_offer(conn, obs())
    assert r.change == "found" and r.old_price is None
    row = conn.execute(
        "SELECT * FROM core.flight_offer WHERE offer_id=%s", (r.offer_id,)
    ).fetchone()
    assert row["price"] == Decimal("54.99") and row["first_seen_at"] == T0
    assert history(conn, r.offer_id) == [{"price": Decimal("54.99"), "cause": "scrape"}]


def test_repeat_scrape_is_observed(conn):
    first = upsert_offer(conn, obs())
    later = T0 + timedelta(hours=6)
    r = upsert_offer(conn, obs(at=later, flight_number="W62048"))
    assert r.change == "observed" and r.offer_id == first.offer_id
    row = conn.execute("SELECT * FROM core.flight_offer").fetchone()
    assert row["last_seen_at"] == later and row["first_seen_at"] == T0
    assert row["flight_number"] == "W62048"
    assert len(history(conn, r.offer_id)) == 2
    assert conn.execute("SELECT count(*) AS n FROM core.flight_offer").fetchone()["n"] == 1


def test_price_change_reports_old_price(conn):
    upsert_offer(conn, obs())
    r = upsert_offer(conn, obs(price="61.49", at=T0 + timedelta(hours=6)))
    assert r.change == "price_changed" and r.old_price == Decimal("54.99")
    assert [h["price"] for h in history(conn, r.offer_id)] == [Decimal("54.99"), Decimal("61.49")]


def test_scrape_does_not_touch_generator_seats(conn):
    r = upsert_offer(conn, obs())
    conn.execute(
        "UPDATE core.flight_offer SET seats_total=200, seats_left=150 WHERE offer_id=%s",
        (r.offer_id,),
    )
    upsert_offer(conn, obs(at=T0 + timedelta(hours=1)))
    row = conn.execute("SELECT seats_total, seats_left FROM core.flight_offer").fetchone()
    assert (row["seats_total"], row["seats_left"]) == (200, 150)
    assert conn.execute(
        "SELECT seats_left FROM core.flight_offer_history ORDER BY id DESC LIMIT 1"
    ).fetchone()["seats_left"] == 150


def test_seats_cannot_go_negative(conn):
    import psycopg
    import pytest

    r = upsert_offer(conn, obs())
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            "UPDATE core.flight_offer SET seats_total=10, seats_left=-1 WHERE offer_id=%s",
            (r.offer_id,),
        )
