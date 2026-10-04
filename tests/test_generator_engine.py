from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from krakow_di.events.publisher import store_events
from krakow_di.generator.engine import Generator, effective_price
from krakow_di.generator.model import CAPACITIES, ModelParams
from krakow_di.repo.flight_offers import OfferObservation, upsert_offer

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)
HIGH = ModelParams(base_rate=0.05, popularity_sigma=1.0)  # rýchle testy: veľa predajov


def seed_offers(conn, n=40, days=(1, 45)):
    """n ponúk na 2 trasy s rôznymi dátumami a cenami."""
    for i in range(n):
        dep = NOW + timedelta(days=days[0] + i * (days[1] - days[0]) / n, minutes=i)
        upsert_offer(conn, OfferObservation(
            source="ryanair", origin_iata="BCN" if i % 2 else "VIE", destination_iata="KRK",
            departure_at=dep, airline_iata="FR", price=Decimal(30 + (i * 7) % 90),
            currency="EUR", observed_at=NOW, flight_number=f"FR{1000 + i}",
        ))
    conn.commit()


def gen(params=HIGH, seed=7):
    return Generator(params, seed, "tuke-di-krakow")


def run_ticks(g, conn, ticks, start=NOW, dt=300):
    events, now = [], start
    for _ in range(ticks):
        now += timedelta(seconds=dt)
        events.extend(g.tick(conn, now, dt).events)
    return events, now


def reset(conn):
    conn.execute("DELETE FROM core.ticket_sale")
    conn.execute("DELETE FROM core.flight_offer_history WHERE cause = 'generator'")
    conn.execute("UPDATE core.flight_offer SET seats_total=NULL, seats_left=NULL, "
                 "price_markup=1, status='active'")
    conn.commit()


def test_first_tick_assigns_capacity_without_events_or_sales(conn):
    seed_offers(conn)
    res = gen().tick(conn, NOW, 0)
    assert res.initialized == 40 and res.events == [] and res.sales == 0
    rows = conn.execute("SELECT seats_total, seats_left, price_markup FROM core.flight_offer"
                        ).fetchall()
    assert all(r["seats_total"] in CAPACITIES and 1 <= r["seats_left"] <= r["seats_total"]
               for r in rows)
    assert all(r["price_markup"] >= 1 for r in rows)
    hist = conn.execute("SELECT count(*) AS n FROM core.flight_offer_history "
                        "WHERE cause='generator' AND status='active'").fetchone()
    assert hist["n"] == 40
    assert gen().tick(conn, NOW, 0).initialized == 0  # druhýkrát už nič neinicializuje


def test_deterministic_for_same_seed_and_different_for_other_seed(conn):
    seed_offers(conn)

    def sales_after(seed):
        reset(conn)
        events, _ = run_ticks(gen(seed=seed), conn, 150)
        rows = conn.execute("SELECT offer_id, quantity, unit_price, sold_at FROM core.ticket_sale "
                            "ORDER BY sold_at, offer_id").fetchall()
        return [(e["event_type"], e["event_id"], e["data"].get("offer_id")) for e in events], rows

    first = sales_after(7)
    assert first[1], "test musí zahŕňať predaje"
    assert sales_after(7) == first
    assert sales_after(8) != first


def test_invariants_over_many_ticks(conn):
    seed_offers(conn)
    g = gen()
    g.tick(conn, NOW, 0)
    initial = {r["offer_id"]: r["seats_total"] - r["seats_left"]
               for r in conn.execute("SELECT * FROM core.flight_offer").fetchall()}
    events, end = run_ticks(g, conn, 600)  # 50 hodín simulovaného času
    assert any(e["event_type"] == "flight.ticket.sold" for e in events)
    store_events(conn, events, "krakow")  # všetky udalosti sú podľa JSON Schema platné

    offers = conn.execute("SELECT * FROM core.flight_offer").fetchall()
    sold = {r["offer_id"]: (r["n"], r["q"]) for r in conn.execute(
        "SELECT offer_id, count(*) AS n, sum(quantity)::int AS q FROM core.ticket_sale "
        "GROUP BY offer_id").fetchall()}
    for o in offers:
        assert 0 <= o["seats_left"] <= o["seats_total"]
        assert o["seats_total"] - o["seats_left"] == initial[o["offer_id"]] + sold.get(
            o["offer_id"], (0, 0))[1]
        if o["seats_left"] == 0:
            assert o["status"] in ("sold_out", "expired")  # po odlete sa vypredaná ponuka uzavrie
        elif o["departure_at"] > end:
            assert o["status"] == "active"
    # žiadny predaj po odlete a žiadny predaj s nulovým počtom
    bad = conn.execute("SELECT count(*) AS n FROM core.ticket_sale s JOIN core.flight_offer o "
                       "USING (offer_id) WHERE s.sold_at >= o.departure_at OR s.quantity < 1"
                       ).fetchone()
    assert bad["n"] == 0
    for e in events:
        if e["event_type"] == "flight.ticket.sold":
            d = e["data"]
            assert d["quantity"] in (1, 2, 3) and d["seats_left_after"] >= 0
            assert d["total_price"] == pytest.approx(d["unit_price"] * d["quantity"], abs=0.01)


def test_dynamic_price_events_follow_thresholds_and_next_sale_uses_new_price(conn):
    seed_offers(conn, n=30, days=(1, 10))
    g = gen(ModelParams(base_rate=0.2, popularity_sigma=1.0))
    events, _ = run_ticks(g, conn, 400)
    changes = [e for e in events if e["event_type"] == "flight.offer.price_changed"]
    assert changes, "rýchla simulácia musí prekročiť prahy obsadenosti"
    for e in changes:
        d = e["data"]
        assert d["reason"] == "load_factor" and e["source"] == "generator"
        assert 1.03 - 0.01 <= d["new_price"] / d["old_price"] <= 1.12 ** 3 + 0.01
    # nasledujúci predaj tej istej ponuky ide za novú cenu
    by_offer: dict[str, list[dict]] = {}
    for e in events:
        if e["event_type"] in ("flight.ticket.sold", "flight.offer.price_changed"):
            by_offer.setdefault(e["data"]["offer_id"], []).append(e)
    checked = 0
    for evs in by_offer.values():
        for prev, nxt in zip(evs, evs[1:], strict=False):
            if prev["event_type"] == "flight.offer.price_changed" \
                    and nxt["event_type"] == "flight.ticket.sold":
                assert nxt["data"]["unit_price"] == prev["data"]["new_price"]
                checked += 1
    assert checked > 0
    # ceny nikdy neklesajú pod cenu zo zdroja
    low = conn.execute("SELECT count(*) AS n FROM core.flight_offer WHERE price_markup < 1"
                       ).fetchone()
    assert low["n"] == 0


def test_sold_out_event_once_and_no_sales_afterwards(conn):
    seed_offers(conn, n=20, days=(1, 4))
    g = gen(ModelParams(base_rate=0.45, popularity_sigma=1.5))
    events, _ = run_ticks(g, conn, 1500)
    sold_out = [e["data"]["offer_id"] for e in events if e["event_type"] == "flight.offer.sold_out"]
    assert sold_out and len(sold_out) == len(set(sold_out))
    for oid in sold_out:
        seq = [e for e in events if e["data"].get("offer_id") == oid]
        idx = next(i for i, e in enumerate(seq) if e["event_type"] == "flight.offer.sold_out")
        assert not any(e["event_type"] == "flight.ticket.sold" for e in seq[idx + 1:])
        row = conn.execute("SELECT status, seats_left FROM core.flight_offer WHERE offer_id=%s",
                           (oid,)).fetchone()
        assert row["seats_left"] == 0 and row["status"] in ("sold_out", "expired")


def test_offers_expire_after_departure_with_event(conn):
    seed_offers(conn, n=10, days=(1, 3))
    g = gen()
    g.tick(conn, NOW, 0)
    res = g.tick(conn, NOW + timedelta(days=10), 300)
    assert res.expired == 10 and res.sales == 0
    assert {e["event_type"] for e in res.events} == {"flight.offer.expired"}
    assert all(e["data"]["reason"] == "departed" for e in res.events)
    assert conn.execute("SELECT count(*) AS n FROM core.flight_offer WHERE status='expired'"
                        ).fetchone()["n"] == 10
    again = g.tick(conn, NOW + timedelta(days=11), 300)
    assert again.expired == 0 and again.events == []  # druhýkrát už nič


def test_run_interval_splits_into_five_minute_ticks_and_stays_in_interval(conn):
    seed_offers(conn)
    g = gen()
    g.tick(conn, NOW, 0)
    res = g.run_interval(conn, NOW, 6 * 3600)  # 6 hodín = 72 tikov
    times = [e["occurred_at"] for e in res.events]
    assert res.sales > 0
    assert min(times) > "2026-10-10T12:00:00Z" and max(times) <= "2026-10-10T18:00:00Z"
    assert times == sorted(times)


def test_speedup_gives_comparable_volume_to_real_ticks(conn):
    seed_offers(conn)
    g = gen()
    g.tick(conn, NOW, 0)
    one = g.run_interval(conn, NOW, 3600)
    reset(conn)
    g.tick(conn, NOW, 0)
    many = sum(g.tick(conn, NOW + timedelta(seconds=300 * (i + 1)), 300).sales for i in range(12))
    assert one.sales > 0 and many > 0
    assert 0.2 < one.sales / many < 5  # rovnaký rád veľkosti (inak náhodné)


def test_scraper_updates_base_price_but_keeps_generator_state(conn):
    seed_offers(conn, n=4, days=(5, 8))
    g = gen()
    g.tick(conn, NOW, 0)
    row = conn.execute("SELECT * FROM core.flight_offer ORDER BY offer_id LIMIT 1").fetchone()
    conn.execute("UPDATE core.flight_offer SET price_markup = 1.10 WHERE offer_id=%s",
                 (row["offer_id"],))
    conn.commit()
    obs = OfferObservation(
        source=row["source"], origin_iata=row["origin_iata"], destination_iata="KRK",
        departure_at=row["departure_at"], airline_iata="FR", price=Decimal("99.00"),
        currency="EUR", observed_at=NOW + timedelta(hours=1))
    upsert_offer(conn, obs)
    conn.commit()
    after = conn.execute("SELECT * FROM core.flight_offer WHERE offer_id=%s",
                         (row["offer_id"],)).fetchone()
    assert (after["seats_total"], after["seats_left"]) == (row["seats_total"], row["seats_left"])
    assert after["price_markup"] == Decimal("1.10000") and after["price"] == Decimal("99.00")
    assert effective_price(after["price"], after["price_markup"]) == Decimal("108.90")


def test_generator_never_touches_raw_data(conn):
    conn.execute("INSERT INTO raw.fetch_log (source, url, status_code, parse_status) "
                 "VALUES ('x', 'u', 200, 'ok')")
    conn.commit()
    seed_offers(conn, n=10)
    run_ticks(gen(), conn, 50)
    rows = conn.execute("SELECT source, url, parse_status FROM raw.fetch_log").fetchall()
    assert rows == [{"source": "x", "url": "u", "parse_status": "ok"}]


def test_two_ticks_at_the_same_instant_never_collide_on_ids(conn):
    """Regresia: id predajov sa nesmú opakovať ani pri dvoch tikoch v tej istej sekunde."""
    seed_offers(conn, n=30, days=(1, 5))
    g = gen(ModelParams(base_rate=0.5, popularity_sigma=1.0))
    g.tick(conn, NOW, 0)
    first = g.tick(conn, NOW + timedelta(minutes=5), 300)
    again = g.tick(conn, NOW + timedelta(minutes=5), 300)  # rovnaký okamih
    assert first.sales > 0 and again.sales > 0
    ids = [e["event_id"] for e in first.events + again.events]
    assert len(ids) == len(set(ids))
    sale_ids = [e["data"]["sale_id"] for e in first.events + again.events
                if e["event_type"] == "flight.ticket.sold"]
    assert len(sale_ids) == len(set(sale_ids))
