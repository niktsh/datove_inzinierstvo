"""Travelpayouts (Aviasales Data API) collector: cached cheapest one-way prices into KRK."""

import argparse
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import psycopg

from krakow_di.collectors.base import OfferChange, RunSummary
from krakow_di.config import Settings, get_settings
from krakow_di.db import connect
from krakow_di.repo.flight_offers import OfferObservation, upsert_offer
from krakow_di.repo.raw import log_fetch, set_parse_status
from krakow_di.routes import RoutesConfig, load_routes

SOURCE = "travelpayouts"
URL = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
RETRY_STATUSES = {429, 500, 502, 503, 504}

log = logging.getLogger(__name__)


def parse_response(
    body: dict, destination: str, observed_at: datetime, max_stops: int | None = None
) -> list[OfferObservation]:
    """Turn one API body into observations: one per offer_id (cheapest wins), no past flights."""
    best: dict[str, OfferObservation] = {}
    for item in body.get("data") or []:
        try:
            dep = datetime.fromisoformat(item["departure_at"])
            airline = item["airline"]
            price = Decimal(str(item["price"]))
            origin = item.get("origin_airport") or item["origin"]
        except (KeyError, ValueError, TypeError, ArithmeticError):
            log.warning("skipping malformed item: %r", item)
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
            raise RuntimeError("TRAVELPAYOUTS_TOKEN is not set")
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
        """GET with retry on 429/5xx/network errors (exponential backoff, honours Retry-After)."""
        headers = {"X-Access-Token": self.settings.travelpayouts_token}
        resp = None
        for attempt in range(self.max_attempts):
            try:
                resp = self.client.get(URL, params=params, headers=headers)
            except httpx.TransportError as e:
                log.warning("network error %s (attempt %d)", type(e).__name__, attempt + 1)
                resp = None
            else:
                if resp.status_code not in RETRY_STATUSES:
                    return resp
                log.warning("HTTP %d (attempt %d)", resp.status_code, attempt + 1)
            if attempt + 1 < self.max_attempts:
                delay = 2.0**attempt
                if resp is not None and resp.headers.get("Retry-After", "").isdigit():
                    delay = max(delay, float(resp.headers["Retry-After"]))
                self.sleep(delay)
        return resp

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
            log.error("%s %s: request failed (status %s)", origin, month, status)
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
    parser = argparse.ArgumentParser(description="Run the Travelpayouts collector once")
    parser.add_argument("--routes", default="config/routes.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    collector = TravelpayoutsCollector(get_settings(), load_routes(args.routes))
    with connect() as conn:
        summary = collector.run(conn)
    print(
        f"requests={summary.requests} errors={summary.errors} empty={summary.empty} "
        f"changes={summary.counts()}"
    )
    print("offers by origin airport:", summary.offers_by_origin())


if __name__ == "__main__":
    main()
