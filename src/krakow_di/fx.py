"""Prepočet na EUR podľa denných referenčných kurzov ECB (1 EUR = N jednotiek meny)."""

import logging
import xml.etree.ElementTree as ET
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

import httpx
import psycopg

from krakow_di.collectors.http import USER_AGENT

ECB_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
_NS = {"e": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}

log = logging.getLogger(__name__)


def parse_ecb_xml(text: str) -> tuple[date, dict[str, Decimal]]:
    root = ET.fromstring(text)
    day_cube = root.find("e:Cube/e:Cube", _NS)
    if day_cube is None:
        raise ValueError("ECB XML: chýba Cube s dátumom")
    rates = {
        c.attrib["currency"]: Decimal(c.attrib["rate"])
        for c in day_cube.findall("e:Cube", _NS)
    }
    return date.fromisoformat(day_cube.attrib["time"]), rates


def to_eur(amount: Decimal, per_eur: Decimal) -> Decimal:
    return (amount / per_eur).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def refresh_rates(conn: psycopg.Connection, client: httpx.Client | None = None) -> date | None:
    """Stiahne kurzy ECB a uloží ich. Vráti dátum kurzov alebo None pri chybe."""
    client = client or httpx.Client(timeout=30, headers={"User-Agent": USER_AGENT})
    try:
        resp = client.get(ECB_URL)
        resp.raise_for_status()
        rate_date, rates = parse_ecb_xml(resp.text)
    except (httpx.HTTPError, ValueError, ET.ParseError) as e:
        log.error("Aktualizácia kurzov ECB zlyhala: %s", e)
        return None
    for cur, rate in rates.items():
        conn.execute(
            "INSERT INTO core.fx_rate (rate_date, currency, per_eur) VALUES (%s, %s, %s) "
            "ON CONFLICT (rate_date, currency) DO NOTHING",
            (rate_date, cur, rate),
        )
    conn.commit()
    return rate_date


def get_rate(
    conn: psycopg.Connection,
    currency: str,
    on: date,
    client: httpx.Client | None = None,
    max_age_days: int = 5,
) -> Decimal | None:
    """Najnovší známy kurz (jednotiek na 1 EUR) nie novší než `on`; zastaraný obnoví z ECB.

    Vráti None, ak sa nenájde dostatočne čerstvý kurz (volajúci uloží price_eur = NULL).
    """
    currency = currency.upper()
    if currency == "EUR":
        return Decimal(1)

    def latest() -> tuple[date, Decimal] | None:
        row = conn.execute(
            "SELECT rate_date, per_eur FROM core.fx_rate WHERE currency = %s AND rate_date <= %s "
            "ORDER BY rate_date DESC LIMIT 1",
            (currency, on),
        ).fetchone()
        return (row["rate_date"], row["per_eur"]) if row else None

    found = latest()
    # ECB publikuje iba v pracovné dni, takže kurz starý pár dní je cez víkend normálny.
    # Ak je kurz zastaraný, opýtame sa ECB, najviac však raz za niekoľko hodín.
    if found is None or (on - found[0]).days > 1:
        recent = conn.execute(
            "SELECT 1 FROM core.fx_rate WHERE fetched_at > now() - interval '6 hours' LIMIT 1"
        ).fetchone()
        if recent is None and refresh_rates(conn, client) is not None:
            found = latest()
    if found is None or (on - found[0]).days > max_age_days:
        return None
    return found[1]
