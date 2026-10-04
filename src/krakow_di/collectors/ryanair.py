"""Zberač Ryanairu: najlacnejšia priama cena na trasu a deň z verejného Fare Finder API.

Endpoint vracia iba jednu najlacnejšiu cenu v požadovanom okne dátumov, preto sa pýtame
po jednom dni. Časy prichádzajú bez posunu od UTC (lokálny čas letiska); posun sa berie
z časového pásma letiska v oficiálnom zozname trás Ryanairu.
"""

import argparse
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import psycopg
import yaml

from krakow_di.collectors.base import OfferChange, RunSummary
from krakow_di.collectors.http import USER_AGENT, get_with_retry
from krakow_di.db import connect
from krakow_di.repo.flight_offers import OfferObservation, upsert_offer
from krakow_di.repo.raw import log_fetch, set_parse_status

SOURCE = "ryanair"
FARES_URL = "https://www.ryanair.com/api/farfnd/v4/oneWayFares"
ROUTES_URL = "https://www.ryanair.com/api/views/locate/searchWidget/routes/en/airport/{dest}"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RyanairConfig:
    destination: str
    destination_tz: str
    horizon_days: int
    currency: str
    market: str
    pause_seconds: float
    max_consecutive_errors: int
    candidates: tuple[str, ...]


def load_config(path: Path | str = "config/routes_ryanair.yaml") -> RyanairConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    candidates = tuple(dict.fromkeys(str(c).upper() for c in raw["candidates"]))
    dest = raw["destination"].upper()
    if dest in candidates:
        raise ValueError("cieľ je uvedený medzi kandidátmi")
    ZoneInfo(raw["destination_tz"])  # zlyhať hneď pri chybnom názve pásma
    return RyanairConfig(
        destination=dest,
        destination_tz=raw["destination_tz"],
        horizon_days=int(raw.get("horizon_days", 90)),
        currency=str(raw.get("currency", "EUR")).upper(),
        market=str(raw.get("market", "en-gb")),
        pause_seconds=float(raw.get("pause_seconds", 1.5)),
        max_consecutive_errors=int(raw.get("max_consecutive_errors", 5)),
        candidates=candidates,
    )


def parse_routes(body: list, candidates: tuple[str, ...]) -> dict[str, str]:
    """Oficiálny zoznam trás -> {IATA letiska: IANA pásmo}, iba naši kandidáti."""
    zones: dict[str, str] = {}
    for item in body:
        try:
            airport = item["arrivalAirport"]
            code, zone = airport["code"], airport["timeZone"]
            ZoneInfo(zone)
        except (KeyError, TypeError, ValueError, LookupError):
            continue
        if code in candidates:
            zones[code] = zone
    return zones


def _local(naive: str, zone: str) -> datetime:
    dt = datetime.fromisoformat(naive)
    if dt.tzinfo is not None:  # obranne: API môže začať posielať posuny
        return dt
    return dt.replace(tzinfo=ZoneInfo(zone))


def parse_fares(
    body: dict,
    origin: str,
    origin_tz: str,
    destination: str,
    destination_tz: str,
    observed_at: datetime,
) -> list[OfferObservation]:
    """Jedno pozorovanie na každú cenu v tele (zvyčajne jedna); chybné/minulé/cudzie preskočí."""
    out: list[OfferObservation] = []
    for fare in body.get("fares") or []:
        try:
            ob = fare["outbound"]
            dep_airport = ob["departureAirport"]["iataCode"]
            arr_airport = ob["arrivalAirport"]["iataCode"]
            number = ob["flightNumber"]
            price = Decimal(str(ob["price"]["value"]))
            currency = ob["price"]["currencyCode"]
            dep = _local(ob["departureDate"], origin_tz)
            arr = _local(ob["arrivalDate"], destination_tz) if ob.get("arrivalDate") else None
        except (KeyError, TypeError, ValueError, ArithmeticError):
            log.warning("preskakujem chybnú cenu: %r", fare)
            continue
        if dep_airport != origin or arr_airport != destination or len(number) < 3:
            continue
        if dep <= observed_at or price < 0:
            continue
        out.append(
            OfferObservation(
                source=SOURCE,
                origin_iata=origin,
                destination_iata=destination,
                departure_at=dep,
                airline_iata=number[:2].upper(),
                price=price,
                currency=currency,
                observed_at=observed_at,
                arrival_at=arr,
                flight_number=number.upper(),
                stops=0,
            )
        )
    return out


class RyanairCollector:
    def __init__(
        self,
        config: RyanairConfig,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        max_attempts: int = 4,
    ):
        self.cfg = config
        self.client = client or httpx.Client(timeout=30, headers=HEADERS)
        self.sleep, self.now, self.max_attempts = sleep, now, max_attempts

    def _get(self, url: str, params: dict) -> httpx.Response | None:
        return get_with_retry(
            self.client, url, params=params, headers=HEADERS,
            max_attempts=self.max_attempts, sleep=self.sleep,
        )

    def load_origins(self, conn: psycopg.Connection) -> dict[str, str]:
        """Aktívne letiská odletu = naši kandidáti, ktoré Ryanair sám uvádza ako trasy do KRK."""
        url = ROUTES_URL.format(dest=self.cfg.destination)
        resp = self._get(url, {})
        body = None
        if resp is not None and resp.status_code == 200:
            try:
                body = resp.json()
            except ValueError:
                body = None
        ok = isinstance(body, list)
        log_fetch(conn, SOURCE, url, None, resp.status_code if resp is not None else None,
                  {"routes": body} if ok else None, "ok" if ok else "error")
        conn.commit()
        if not ok:
            raise RuntimeError("nepodarilo sa načítať zoznam trás Ryanairu")
        zones = parse_routes(body, self.cfg.candidates)
        missing = [c for c in self.cfg.candidates if c not in zones]
        if missing:
            log.info("kandidáti bez trasy Ryanairu do %s: %s", self.cfg.destination, missing)
        return zones

    def run(
        self,
        conn: psycopg.Connection,
        origins: tuple[str, ...] | None = None,
        horizon_days: int | None = None,
    ) -> RunSummary:
        summary = RunSummary()
        zones = self.load_origins(conn)
        selected = [o for o in (origins or tuple(zones)) if o in zones]
        horizon = horizon_days or self.cfg.horizon_days
        today = self.now().date()
        dates = [today + timedelta(days=i) for i in range(horizon)]
        consecutive = 0
        first = True
        for origin in selected:
            for day in dates:
                if not first:
                    self.sleep(self.cfg.pause_seconds)
                first = False
                failed = self._collect_one(conn, origin, zones[origin], day, summary)
                consecutive = consecutive + 1 if failed else 0
                if consecutive >= self.cfg.max_consecutive_errors:
                    summary.aborted = True
                    log.error("%d chýb za sebou, zastavujem (možná blokácia)", consecutive)
                    return summary
        return summary

    def _collect_one(
        self, conn: psycopg.Connection, origin: str, zone: str, day: date, summary: RunSummary
    ) -> bool:
        """Stiahne jedno letisko/deň. Vráti True, ak dopyt zlyhal."""
        params = {
            "departureAirportIataCode": origin,
            "arrivalAirportIataCode": self.cfg.destination,
            "outboundDepartureDateFrom": day.isoformat(),
            "outboundDepartureDateTo": day.isoformat(),
            "market": self.cfg.market,
            "currency": self.cfg.currency,
        }
        summary.requests += 1
        resp = self._get(FARES_URL, params)
        body = None
        if resp is not None and resp.status_code == 200:
            try:
                body = resp.json()
            except ValueError:
                body = None
        ok = isinstance(body, dict) and isinstance(body.get("fares"), list)
        fetch_id = log_fetch(
            conn, SOURCE, FARES_URL, params, resp.status_code if resp is not None else None,
            body if ok else None, "pending" if ok else "error",
        )
        if not ok:
            summary.errors += 1
            conn.commit()
            log.error("%s %s: dopyt zlyhal (stav %s)", origin, day,
                      resp.status_code if resp is not None else None)
            return True
        observations = parse_fares(
            body, origin, zone, self.cfg.destination, self.cfg.destination_tz, self.now()
        )
        if not body["fares"]:
            summary.empty += 1
            set_parse_status(conn, fetch_id, "empty")
        else:
            for obs in observations:
                summary.changes.append(OfferChange(obs, upsert_offer(conn, obs)))
            set_parse_status(conn, fetch_id, "ok")
        conn.commit()
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Jedenkrát spustí zberač Ryanairu")
    parser.add_argument("--config", default="config/routes_ryanair.yaml")
    parser.add_argument("--publish", action="store_true", help="odoslať udalosti do Kafky")
    parser.add_argument("--origins", help="podmnožina oddelená čiarkou, napr. BCN,VIE")
    parser.add_argument("--days", type=int, help="prepíše horizon_days (na rýchle kontroly)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    collector = RyanairCollector(load_config(args.config))
    origins = tuple(args.origins.upper().split(",")) if args.origins else None
    with connect() as conn:
        summary = collector.run(conn, origins, args.days)
        if args.publish:
            from krakow_di.config import get_settings
            from krakow_di.events.builders import offer_events
            from krakow_di.events.publisher import publish_events

            events = offer_events(conn, summary.changes, get_settings().producer_id)
            print("udalosti (uložené, odoslané):", publish_events(conn, events))
    print(
        f"requests={summary.requests} errors={summary.errors} empty={summary.empty} "
        f"aborted={summary.aborted} changes={summary.counts()}"
    )
    print("ponuky podľa letiska odletu:", summary.offers_by_origin())


if __name__ == "__main__":
    main()
