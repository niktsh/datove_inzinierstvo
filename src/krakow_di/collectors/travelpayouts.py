"""Zberač Travelpayouts (Aviasales Data API): najlacnejšie ceny jedným smerom do KRK z cache."""

import argparse
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import psycopg

from krakow_di.collectors.base import OfferChange, RunSummary
from krakow_di.collectors.http import get_with_retry
from krakow_di.config import Settings, get_settings
from krakow_di.db import connect
from krakow_di.repo.flight_offers import OfferObservation, upsert_offer
from krakow_di.repo.raw import log_fetch, set_parse_status
from krakow_di.routes import RoutesConfig, load_routes

SOURCE = "travelpayouts"
URL = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"

log = logging.getLogger(__name__)


def parse_response(
    body: dict, destination: str, observed_at: datetime, max_stops: int | None = None
) -> list[OfferObservation]:
    """Premení telo odpovede API na pozorovania: jedno na offer_id (najlacnejšie), bez minulých."""
    best: dict[str, OfferObservation] = {}
    for item in body.get("data") or []:
        try:
            dep = datetime.fromisoformat(item["departure_at"])
            airline = item["airline"]
            price = Decimal(str(item["price"]))
            origin = item.get("origin_airport") or item["origin"]
        except (KeyError, ValueError, TypeError, ArithmeticError):
            log.warning("preskakujem chybnú položku: %r", item)
            continue
        if dep.tzinfo is None or dep <= observed_at:
            continue
        if item.get("destination_airport", item.get("destination")) != destination:
            continue
        stops = int(item.get("transfers") or 0)
        if max_stops is not None and stops > max_stops:
            continue
        duration = item.get("duration_to") or 0
        number = item.get("flight_number")
        obs = OfferObservation(
            source=SOURCE,
            origin_iata=origin,
            destination_iata=destination,
            departure_at=dep,
            airline_iata=airline,
            price=price,
            currency=str(body.get("currency") or "eur").upper(),
            observed_at=observed_at,
            arrival_at=dep + timedelta(minutes=duration) if duration and stops == 0 else None,
            flight_number=f"{airline}{number}" if number else None,
            stops=stops,
        )
        known = best.get(obs.offer_id)
        if known is None or obs.price < known.price:
            best[obs.offer_id] = obs
    return list(best.values())


class TravelpayoutsCollector:
    def __init__(
        self,
        settings: Settings,
        routes: RoutesConfig,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        pause: float = 0.3,
        max_attempts: int = 4,
    ):
        if not settings.travelpayouts_token:
            raise RuntimeError("TRAVELPAYOUTS_TOKEN nie je nastavený")
        self.settings, self.routes = settings, routes
        self.client = client or httpx.Client(timeout=30)
        self.sleep, self.now, self.pause, self.max_attempts = sleep, now, pause, max_attempts

    def _params(self, origin: str, month: str) -> dict:
        return {
            "origin": origin,
            "destination": self.routes.destination,
            "departure_at": month,
            "one_way": "true",
            "currency": self.routes.currency,
            "market": self.settings.travelpayouts_market,
            "sorting": "price",
            "limit": 1000,
        }

    def fetch(self, params: dict) -> httpx.Response | None:
        return get_with_retry(
            self.client, URL, params=params,
            headers={"X-Access-Token": self.settings.travelpayouts_token},
            max_attempts=self.max_attempts, sleep=self.sleep,
        )

    def run(self, conn: psycopg.Connection) -> RunSummary:
        summary = RunSummary()
        today = self.now().date()
        first = True
        for origin in self.routes.origins:
            for month in self.routes.months(today):
                if not first:
                    self.sleep(self.pause)
                first = False
                self._collect_one(conn, origin, month, summary)
        return summary

    def _collect_one(
        self, conn: psycopg.Connection, origin: str, month: str, summary: RunSummary
    ) -> None:
        params = self._params(origin, month)
        summary.requests += 1
        resp = self.fetch(params)
        body = None
        if resp is not None and resp.status_code == 200:
            try:
                body = resp.json()
            except ValueError:
                body = None
        status = resp.status_code if resp is not None else None
        ok = isinstance(body, dict) and body.get("success") is not False
        fetch_id = log_fetch(
            conn, SOURCE, URL, params, status, body if isinstance(body, dict) else None,
            "pending" if ok else "error",
        )
        if not ok:
            summary.errors += 1
            conn.commit()
            log.error("%s %s: dopyt zlyhal (stav %s)", origin, month, status)
            return
        observed_at = self.now()
        observations = parse_response(
            body, self.routes.destination, observed_at, self.routes.max_stops
        )
        if not (body.get("data") or []):
            summary.empty += 1
            set_parse_status(conn, fetch_id, "empty")
        else:
            for obs in observations:
                summary.changes.append(OfferChange(obs, upsert_offer(conn, obs)))
            set_parse_status(conn, fetch_id, "ok")
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Jedenkrát spustí zberač Travelpayouts")
    parser.add_argument("--routes", default="config/routes.yaml")
    parser.add_argument("--publish", action="store_true", help="odoslať udalosti do Kafky")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    collector = TravelpayoutsCollector(get_settings(), load_routes(args.routes))
    with connect() as conn:
        summary = collector.run(conn)
        if args.publish:
            from krakow_di.events.builders import offer_events
            from krakow_di.events.publisher import publish_events

            events = offer_events(conn, summary.changes, get_settings().producer_id)
            print("udalosti (uložené, odoslané):", publish_events(conn, events))
    print(
        f"requests={summary.requests} errors={summary.errors} empty={summary.empty} "
        f"changes={summary.counts()}"
    )
    print("ponuky podľa letiska odletu:", summary.offers_by_origin())


if __name__ == "__main__":
    main()
