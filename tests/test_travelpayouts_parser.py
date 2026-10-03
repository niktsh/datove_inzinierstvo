import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from krakow_di.collectors.travelpayouts import parse_response

FIX = Path(__file__).parent / "fixtures" / "travelpayouts"
NOW = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)


def load(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def item(**kw):
    base = {
        "flight_number": "3035", "origin_airport": "BCN", "destination_airport": "KRK",
        "origin": "BCN", "destination": "KRK", "airline": "FR", "price": 43,
        "departure_at": "2026-10-21T18:35:00+02:00", "duration_to": 170, "transfers": 0,
    }
    return {"currency": "eur", "success": True, "data": [{**base, **kw}]}


def test_real_fixture_direct_only():
    body = load("bcn_2026-10.json")
    all_ = parse_response(body, "KRK", NOW)
    direct = parse_response(body, "KRK", NOW, max_stops=0)
    assert 0 < len(direct) < len(all_)
    assert all(o.stops == 0 and o.currency == "EUR" for o in direct)
    assert all(o.departure_at > NOW and o.destination_iata == "KRK" for o in direct)
    assert len({o.offer_id for o in all_}) == len(all_)


def test_field_mapping():
    (o,) = parse_response(item(), "KRK", NOW)
    assert o.origin_iata == "BCN" and o.airline_iata == "FR" and o.flight_number == "FR3035"
    assert o.price == Decimal("43") and o.stops == 0
    assert o.arrival_at.isoformat() == "2026-10-21T21:25:00+02:00"
    assert o.source == "travelpayouts"


def test_uses_airport_not_city_code():
    (o,) = parse_response(item(origin="BRU", origin_airport="CRL"), "KRK", NOW)
    assert o.origin_iata == "CRL"


def test_empty_and_malformed():
    assert parse_response(load("empty.json"), "KRK", NOW) == []
    assert parse_response({"success": True}, "KRK", NOW) == []
    assert parse_response(item(price=None), "KRK", NOW) == []
    assert parse_response(item(departure_at="garbage"), "KRK", NOW) == []


def test_past_and_wrong_destination_skipped():
    assert parse_response(item(departure_at="2026-10-03T13:00:00+01:00"), "KRK", NOW) == []
    assert parse_response(item(destination_airport="WAW"), "KRK", NOW) == []


def test_missing_flight_number_is_none_and_no_arrival_for_connections():
    (o,) = parse_response(item(flight_number=None, transfers=1), "KRK", NOW)
    assert o.flight_number is None and o.arrival_at is None and o.stops == 1


def test_duplicates_keep_cheapest():
    body = item()
    body["data"] += [
        {**body["data"][0], "price": 39, "flight_number": "3099"},
        {**body["data"][0], "price": 60, "flight_number": "3100"},
    ]
    (o,) = parse_response(body, "KRK", NOW)
    assert o.price == Decimal("39") and o.flight_number == "FR3099"
