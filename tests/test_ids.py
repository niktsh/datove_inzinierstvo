from datetime import UTC, datetime, timedelta, timezone

import pytest

from krakow_di.ids import make_offer_id

DEP = datetime(2026, 11, 20, 6, 15, tzinfo=timezone(timedelta(hours=1)))


def test_deterministic_and_16_hex():
    a = make_offer_id("wizzair", "BCN", "KRK", DEP, "W6")
    assert a == make_offer_id("wizzair", "BCN", "KRK", DEP, "W6")
    assert len(a) == 16 and int(a, 16) >= 0


def test_truncates_to_minute_and_ignores_timezone():
    base = make_offer_id("wizzair", "BCN", "KRK", DEP, "W6")
    assert base == make_offer_id("wizzair", "BCN", "KRK", DEP.replace(second=59), "W6")
    assert base == make_offer_id("wizzair", "BCN", "KRK", DEP.astimezone(UTC), "W6")
    assert base != make_offer_id("wizzair", "BCN", "KRK", DEP + timedelta(minutes=1), "W6")


def test_case_insensitive_and_sensitive_to_each_field():
    base = make_offer_id("wizzair", "BCN", "KRK", DEP, "W6")
    assert base == make_offer_id("WizzAir", "bcn", "krk", DEP, "w6")
    assert base != make_offer_id("travelpayouts", "BCN", "KRK", DEP, "W6")
    assert base != make_offer_id("wizzair", "VIE", "KRK", DEP, "W6")
    assert base != make_offer_id("wizzair", "BCN", "WAW", DEP, "W6")
    assert base != make_offer_id("wizzair", "BCN", "KRK", DEP, "FR")


def test_naive_datetime_rejected():
    with pytest.raises(ValueError):
        make_offer_id("wizzair", "BCN", "KRK", datetime(2026, 11, 20, 6, 15), "W6")
