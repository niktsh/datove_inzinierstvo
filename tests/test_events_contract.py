import json
import re
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator

from krakow_di.events.envelope import (
    SCHEMA_DIR,
    InvalidEvent,
    iso_local,
    iso_utc,
    make_event,
    validate,
)
from krakow_di.events.routing import ALL_EVENT_TYPES, TOPIC_SPECS, key_for, topic_for

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 14, 9, 31, 5, tzinfo=UTC)

OFFER = {
    "offer_id": "a1b2c3d4e5f60718", "origin_iata": "BCN", "destination_iata": "KRK",
    "departure_at": "2026-11-20T06:15:00+01:00", "arrival_at": "2026-11-20T09:10:00+01:00",
    "airline_iata": "FR", "flight_number": "FR3035", "stops": 0, "price": 54.99,
    "currency": "EUR", "seats_total": 189, "seats_left": 141, "seats_simulated": True,
    "observed_at": "2026-10-14T06:00:12Z",
}
PERF = {
    "performance_id": "wielki-gatsby-2026-11-03-19-00", "title": "Wielki Gatsby",
    "stage": "Scena MOS", "starts_at": "2026-11-03T19:00:00+01:00",
    "url": "https://bilety.teatrwkrakowie.pl/kup-bilet/wielki-gatsby-2026-11-03-19-00",
    "status": "on_sale",
}
SAMPLES = {
    "flight.offer.found": ("travelpayouts", OFFER),
    "flight.offer.observed": ("ryanair", OFFER),
    "flight.offer.price_changed": ("ryanair", {
        "offer_id": "a1b2c3d4e5f60718", "old_price": 54.99, "new_price": 61.49,
        "currency": "EUR", "reason": "scrape"}),
    "flight.ticket.sold": ("generator", {
        "sale_id": "5d0c9f3a-1111-4222-8333-444455556666", "offer_id": "a1b2c3d4e5f60718",
        "origin_iata": "BCN", "destination_iata": "KRK",
        "departure_at": "2026-11-20T06:15:00+01:00", "airline_iata": "FR",
        "flight_number": "FR3035", "quantity": 2, "unit_price": 54.99, "total_price": 109.98,
        "currency": "EUR", "seats_left_after": 139, "sold_at": "2026-10-14T09:31:05Z"}),
    "flight.offer.sold_out": ("generator", {
        "offer_id": "a1b2c3d4e5f60718", "last_price": 89.99, "currency": "EUR"}),
    "flight.offer.expired": ("generator", {"offer_id": "a1b2c3d4e5f60718", "reason": "departed"}),
    "theater.performance.found": ("theater", PERF),
    "theater.performance.updated": ("theater", {**PERF, "url": None, "status": "sold_out"}),
    "theater.availability.snapshot": ("theater", {
        "performance_id": PERF["performance_id"], "observed_at": "2026-10-14T12:00:03Z",
        "fx_rate": 4.3775, "seats_available_total": 13,
        "categories": [
            {"category": "Normalny", "price": 120.0, "currency": "PLN", "price_eur": 27.41,
             "seats_available": 4},
            {"category": "strefa C", "price": 50.0, "currency": "PLN", "price_eur": None,
             "seats_available": 9}]}),
    "theater.tickets.sold": ("theater", {
        "performance_id": PERF["performance_id"], "category": "Normalny", "quantity": -1,
        "unit_price": 120.0, "currency": "PLN", "unit_price_eur": 27.41, "fx_rate": 4.3775,
        "detected_between": ["2026-10-14T08:00:01Z", "2026-10-14T12:00:03Z"]}),
}


def test_every_event_type_has_a_valid_schema_and_a_sample():
    files = {p.stem for p in SCHEMA_DIR.glob("*.json")}
    assert files == set(ALL_EVENT_TYPES) == set(SAMPLES)
    for et in ALL_EVENT_TYPES:
        schema = json.loads((SCHEMA_DIR / f"{et}.json").read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)


@pytest.mark.parametrize("event_type", ALL_EVENT_TYPES)
def test_sample_event_is_valid(event_type):
    source, data = SAMPLES[event_type]
    event = make_event(event_type, source, data, NOW, "tuke-di-krakow")
    assert event["event_version"] == 1 and event["occurred_at"] == "2026-10-14T09:31:05Z"
    assert event["event_id"] and event["producer"] == "tuke-di-krakow"
    assert key_for(event) == data[next(s.key_field for s in TOPIC_SPECS
                                       if event_type in s.event_types)]


@pytest.mark.parametrize(
    "event_type,mutate",
    [
        ("flight.offer.found", lambda d: d.pop("price")),
        ("flight.offer.found", lambda d: d.update(origin_iata="bcn")),
        ("flight.offer.found", lambda d: d.update(offer_id="XYZ")),
        ("flight.offer.found", lambda d: d.update(seats_left=-1)),
        ("flight.offer.found", lambda d: d.update(departure_at="zajtra")),
        ("flight.offer.price_changed", lambda d: d.update(reason="magic")),
        ("flight.ticket.sold", lambda d: d.update(quantity=0)),
        ("flight.ticket.sold", lambda d: d.update(sale_id="nie-uuid")),
        ("theater.tickets.sold", lambda d: d.update(quantity=0)),
        ("theater.tickets.sold", lambda d: d.update(detected_between=["2026-10-14T08:00:01Z"])),
        ("theater.performance.found", lambda d: d.update(status="whatever")),
        ("theater.availability.snapshot", lambda d: d["categories"][0].update(seats_available=-3)),
    ],
)
def test_invalid_data_is_rejected(event_type, mutate):
    source, data = SAMPLES[event_type]
    data = json.loads(json.dumps(data))
    mutate(data)
    with pytest.raises(InvalidEvent):
        make_event(event_type, source, data, NOW, "p")


def test_wrong_source_unknown_type_and_missing_envelope_fields():
    with pytest.raises(InvalidEvent):
        make_event("flight.ticket.sold", "ryanair", SAMPLES["flight.ticket.sold"][1], NOW, "p")
    with pytest.raises(InvalidEvent):
        validate({"data": {}})
    with pytest.raises(InvalidEvent):
        validate({"event_type": "flight.offer.teleported"})
    event = make_event("flight.offer.sold_out", "generator",
                       SAMPLES["flight.offer.sold_out"][1], NOW, "p")
    del event["producer"]
    with pytest.raises(InvalidEvent):
        validate(event)


def test_extra_optional_fields_keep_the_same_version():
    source, data = SAMPLES["flight.offer.found"]
    make_event("flight.offer.found", source, {**data, "novinka": 1}, NOW, "p")


def test_time_helpers():
    assert iso_utc(datetime(2026, 11, 20, 6, 15, tzinfo=timezone(timedelta(hours=1)))) == (
        "2026-11-20T05:15:00Z")
    assert iso_local(datetime(2026, 11, 20, 6, 15, tzinfo=timezone(timedelta(hours=1)))) == (
        "2026-11-20T06:15:00+01:00")
    with pytest.raises(ValueError):
        iso_utc(datetime(2026, 1, 1))


def test_topics_match_the_documented_contract():
    expected = {
        "krakow.flights.offers": (3, "offer_id"), "krakow.flights.sales": (3, "offer_id"),
        "krakow.theater.performances": (1, "performance_id"),
        "krakow.theater.availability": (1, "performance_id"),
        "krakow.theater.sales": (1, "performance_id"),
    }
    assert {f"krakow.{s.suffix}": (s.partitions, s.key_field) for s in TOPIC_SPECS} == expected
    assert topic_for("flight.ticket.sold", "krakow") == "krakow.flights.sales"
    with pytest.raises(ValueError):
        topic_for("nope", "krakow")


def test_asyncapi_is_consistent_with_code_and_schemas():
    doc = yaml.safe_load((ROOT / "docs" / "asyncapi.yaml").read_text(encoding="utf-8"))
    addresses = {c["address"]: set(c["messages"]) for c in doc["channels"].values()}
    assert addresses == {f"krakow.{s.suffix}": set(s.event_types) for s in TOPIC_SPECS}
    messages = doc["components"]["messages"]
    assert set(messages) == set(ALL_EVENT_TYPES)
    for et, msg in messages.items():
        target = (ROOT / "docs" / msg["payload"]["$ref"]).resolve()
        assert target == (SCHEMA_DIR / f"{et}.json").resolve() and target.is_file()
        assert msg["headers"]["properties"]["event_type"]["const"] == et


def test_udalosti_md_documents_every_event_type_and_topic():
    text = (ROOT / "docs" / "UDALOSTI.md").read_text(encoding="utf-8")
    for et in ALL_EVENT_TYPES:
        assert f"`{et}`" in text, et
    for s in TOPIC_SPECS:
        assert f"`krakow.{s.suffix}`" in text, s.suffix
    assert re.search(r"\|\s*`source`\s*\|[^\n]*ryanair", text)
