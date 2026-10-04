from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx

from krakow_di.fx import get_rate, parse_ecb_xml, refresh_rates, to_eur

XML = (Path(__file__).parent / "fixtures" / "slowacki" / "ecb_daily.xml").read_text()


def test_parse_ecb_xml():
    d, rates = parse_ecb_xml(XML)
    assert d == date(2026, 10, 2) and rates["PLN"] == Decimal("4.3775")


def test_to_eur_rounds_half_up():
    assert to_eur(Decimal("100.00"), Decimal("4.3775")) == Decimal("22.84")
    assert to_eur(Decimal("120"), Decimal("4")) == Decimal("30.00")


def client(calls):
    def handler(req):
        calls.append(str(req.url))
        return httpx.Response(200, text=XML)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_get_rate_eur_is_one_without_network(conn):
    calls = []
    assert get_rate(conn, "EUR", date(2026, 10, 3), client(calls)) == Decimal(1)
    assert calls == []


def test_get_rate_downloads_once_then_uses_db(conn):
    calls = []
    c = client(calls)
    assert get_rate(conn, "PLN", date(2026, 10, 4), c) == Decimal("4.377500")
    assert get_rate(conn, "pln", date(2026, 10, 4), c) == Decimal("4.377500")
    assert len(calls) == 1  # nedeľa: piatkový kurz je zastaraný, ale ECB sa dnes už pýtalo


def test_get_rate_none_when_too_old_or_unavailable(conn):
    calls = []
    assert get_rate(conn, "PLN", date(2026, 11, 30), client(calls)) is None  # kurz starý 2 mesiace
    broken = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    assert get_rate(conn, "SEK", date(2026, 10, 3), broken) is None  # nikdy neuložené
    assert refresh_rates(conn, broken) is None
