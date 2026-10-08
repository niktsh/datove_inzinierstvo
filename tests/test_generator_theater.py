from datetime import UTC, datetime, timedelta
from decimal import Decimal

from krakow_di.events.publisher import store_events
from krakow_di.generator.model import (
    THEATER_SALE_SIZES,
    TheaterParams,
    theater_sale_probability,
    theater_time_factor,
)
from krakow_di.generator.theater import TheaterGenerator
from krakow_di.repo import theater as repo

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)
HIGH = TheaterParams(base_rate=0.2, popularity_sigma=1.0)  # rýchle testy: veľa predajov


def seed_theater(conn, n=6, seats=(8, 3)):
    """n predstavení v najbližších týždňoch, každé s dvoma kategóriami a jednou reálnou snímkou."""
    for i in range(n):
        pid = f"hra-{i}-2026-10-{20 + i}-19-00"
        repo.upsert_performance(conn, repo.Performance(
            pid, f"Hra {i}", "Scena MOS", NOW + timedelta(days=5 + 3 * i),
            f"https://bilety.example/kup-bilet/{pid}"), NOW)
        cats = [repo.CategorySeats("Normalny", Decimal("120.00"), seats[0]),
                repo.CategorySeats("Strefa C", Decimal("50.00"), seats[1])]
        repo.insert_snapshot(conn, pid, NOW, cats, "PLN", Decimal("4.3775"),
                             {("Normalny", Decimal("120.00")): Decimal("27.41"),
                              ("Strefa C", Decimal("50.00")): Decimal("11.42")})
    conn.commit()


def gen(params=HIGH, seed=7):
    return TheaterGenerator(params, seed, "tuke-di-krakow")


def run_ticks(g, conn, ticks, dt=300):
    events, now = [], NOW
    for _ in range(ticks):
        now += timedelta(seconds=dt)
        events.extend(g.tick(conn, now, dt).events)
    return events, now


def reset(conn):
    conn.execute("DELETE FROM core.theater_sale")
    conn.commit()


def test_sales_are_marked_as_generator_and_valid(conn):
    seed_theater(conn)
    events, _ = run_ticks(gen(), conn, 20)
    assert events
    rows = conn.execute("SELECT * FROM core.theater_sale").fetchall()
    assert len(rows) == len(events)
    assert all(r["source"] == "generator" and r["quantity"] in THEATER_SALE_SIZES for r in rows)
    assert all(r["unit_price_eur"] is not None and r["fx_rate"] is not None for r in rows)
    for e in events:  # make_event už validoval podľa JSON Schema
        assert e["event_type"] == "theater.tickets.sold" and e["source"] == "generator"
        assert e["data"]["simulated"] is True and e["data"]["quantity"] > 0
    assert store_events(conn, events, "krakow") == len(events)


def test_same_seed_gives_same_sales(conn):
    seed_theater(conn)
    first, _ = run_ticks(gen(), conn, 15)
    reset(conn)
    second, _ = run_ticks(gen(), conn, 15)
    assert first and first == second
    reset(conn)
    other, _ = run_ticks(gen(seed=8), conn, 15)
    assert other != first


def test_seats_never_go_negative(conn):
    seed_theater(conn, n=3, seats=(5, 2))
    run_ticks(gen(TheaterParams(base_rate=0.5)), conn, 200)
    left = conn.execute(
        "SELECT s.seats_available - COALESCE(sum(x.quantity), 0) AS left "
        "FROM core.theater_snapshot s LEFT JOIN core.theater_sale x "
        "ON x.performance_id = s.performance_id AND x.category = s.category "
        "GROUP BY s.performance_id, s.category, s.seats_available").fetchall()
    assert left and all(r["left"] >= 0 for r in left)
    total = conn.execute("SELECT sum(quantity) AS n FROM core.theater_sale").fetchone()["n"]
    assert total == 3 * (5 + 2)  # pri vysokej intenzite sa miesta vypredajú do posledného


def test_no_sales_for_past_or_unsnapshotted_performances(conn):
    seed_theater(conn, n=2)
    repo.upsert_performance(conn, repo.Performance(  # bez snímky
        "bez-snimky-2026-10-30-19-00", "Bez snímky", None, NOW + timedelta(days=20),
        "https://bilety.example/x"), NOW)
    conn.commit()
    events, _ = run_ticks(gen(), conn, 50)
    assert {e["data"]["performance_id"] for e in events} <= {
        "hra-0-2026-10-20-19-00", "hra-1-2026-10-21-19-00"}
    late = NOW + timedelta(days=30)  # po termíne všetkých predstavení
    assert gen().tick(conn, late, 300).sales == 0


def test_new_real_snapshot_does_not_change_observed_sales(conn):
    """Simulácia sa nemieša do detekcie: reálna snímka je nezávislá od simulovaných predajov."""
    seed_theater(conn, n=1)
    run_ticks(gen(), conn, 30)
    pid = "hra-0-2026-10-20-19-00"
    prev_at, prev = repo.previous_snapshot(conn, pid)
    assert prev_at == NOW and prev[("Normalny", Decimal("120.00"))] == 8


def test_model_shape():
    assert theater_time_factor(0) > theater_time_factor(30)
    p = TheaterParams()
    assert theater_sale_probability(p, 1.0, 1) > theater_sale_probability(p, 1.0, 60)
    assert theater_sale_probability(p, 1.0, 1, 0) == 0
