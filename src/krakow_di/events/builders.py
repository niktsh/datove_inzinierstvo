"""Premena výsledkov zberačov a generátora na udalosti podľa docs/UDALOSTI.md."""

import psycopg

from krakow_di.collectors.base import OfferChange
from krakow_di.collectors.slowacki import TheaterRun
from krakow_di.events.envelope import iso_local, iso_utc, make_event


def _f(value) -> float | None:
    return None if value is None else float(value)


def offer_events(conn: psycopg.Connection, changes: list[OfferChange], producer: str) -> list[dict]:
    """found / observed (+ price_changed pri zmene ceny) pre každú zmenu zo zberu.

    Miesta (seats_*) patria generátoru, preto sa berú zo stavu v DB (pred jeho spustením null).
    """
    if not changes:
        return []
    seats = {
        r["offer_id"]: r
        for r in conn.execute(
            "SELECT offer_id, seats_total, seats_left FROM core.flight_offer "
            "WHERE offer_id = ANY(%s)",
            ([c.result.offer_id for c in changes],),
        ).fetchall()
    }
    events: list[dict] = []
    for ch in changes:
        o, r = ch.observation, ch.result
        row = seats.get(r.offer_id, {})
        data = {
            "offer_id": r.offer_id,
            "origin_iata": o.origin_iata,
            "destination_iata": o.destination_iata,
            "departure_at": iso_local(o.departure_at),
            "arrival_at": iso_local(o.arrival_at) if o.arrival_at else None,
            "airline_iata": o.airline_iata,
            "flight_number": o.flight_number,
            "stops": o.stops,
            "price": _f(o.price),
            "currency": o.currency,
            "seats_total": row.get("seats_total"),
            "seats_left": row.get("seats_left"),
            "seats_simulated": True,
            "observed_at": iso_utc(o.observed_at),
        }
        kind = "found" if r.change == "found" else "observed"
        events.append(make_event(f"flight.offer.{kind}", o.source, data, o.observed_at, producer))
        if r.change == "price_changed":
            events.append(make_event(
                "flight.offer.price_changed", o.source,
                {"offer_id": r.offer_id, "old_price": _f(r.old_price), "new_price": _f(o.price),
                 "currency": o.currency, "reason": "scrape"},
                o.observed_at, producer,
            ))
    return events


def _performance_data(p) -> dict:
    return {
        "performance_id": p.performance_id,
        "title": p.title,
        "stage": p.stage,
        "starts_at": iso_local(p.starts_at),
        "url": p.url,
        "status": p.status,
    }


def theater_events(run: TheaterRun, producer: str, observed_at) -> list[dict]:
    """Programa (found/updated), snímky dostupnosti a zistené predaje z jedného behu zberača."""
    events: list[dict] = []
    for kind, items in (("found", run.found), ("updated", run.updated)):
        for p in items:
            events.append(make_event(
                f"theater.performance.{kind}", "theater", _performance_data(p), observed_at,
                producer,
            ))
    for s in run.snapshots:
        cats = [
            {"category": c.name, "price": _f(c.price), "currency": "PLN",
             "price_eur": _f(s.eur_prices.get((c.name, c.price))),
             "seats_available": c.seats_available}
            for c in s.categories
        ]
        events.append(make_event(
            "theater.availability.snapshot", "theater",
            {"performance_id": s.performance_id, "observed_at": iso_utc(s.observed_at),
             "fx_rate": _f(s.fx_rate), "categories": cats,
             "seats_available_total": sum(c.seats_available for c in s.categories)},
            s.observed_at, producer,
        ))
    for sale in run.sales:
        events.append(make_event(
            "theater.tickets.sold", "theater",
            {"performance_id": sale.performance_id, "category": sale.category,
             "quantity": sale.quantity, "unit_price": _f(sale.unit_price),
             "currency": sale.currency, "unit_price_eur": _f(sale.unit_price_eur),
             "fx_rate": _f(sale.fx_rate),
             "detected_between": [iso_utc(sale.detected_from), iso_utc(sale.detected_to)]},
            sale.detected_to, producer,
        ))
    return events
