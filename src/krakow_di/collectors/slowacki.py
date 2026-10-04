"""Teatr im. J. Słowackiego (Kraków): program, dostupnosť miest a zistené predaje.

Zoznam programu aj mapa miest sú interné endpointy, ktoré používa samotný web divadla
(pozri docs/zdroje/divadlo.md). Prihlásenie netreba.
"""

import argparse
import html
import json
import logging
import re
import time
import unicodedata
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import httpx
import psycopg

from krakow_di.collectors.http import USER_AGENT, get_with_retry
from krakow_di.config import get_settings
from krakow_di.db import connect
from krakow_di.fx import get_rate, to_eur
from krakow_di.repo import theater as repo
from krakow_di.repo.raw import log_fetch, set_parse_status
from krakow_di.repo.theater import CategorySeats, Performance, Sale

SOURCE = "theater"
LIST_URL = "https://teatrwkrakowie.pl/ajax/pl/repertoireList"
SEATS_URL = "https://bilety.teatrwkrakowie.pl/sbLocationService/forSale.json"
LIST_REFERER = "https://teatrwkrakowie.pl/repertuar"
CURRENCY = "PLN"
TZ = ZoneInfo("Europe/Warsaw")

log = logging.getLogger(__name__)

_BLOCK_SPLIT = re.compile(r'<div class="block"')
_TITLE = re.compile(r"<h2[^>]*>\s*<a[^>]*>\s*(.*?)\s*</a>", re.S)
_STAGE = re.compile(r'href="\s*/sceny/[^"]*?\s*"[^>]*>\s*(.*?)\s*</a>', re.S)
_TICKET = re.compile(r'https://bilety\.teatrwkrakowie\.pl/kup-bilet/([a-z0-9_-]+)')
_INSTANCE = re.compile(r'data-instance-tickets="(\d+)"')
_BLOCK_DATE = re.compile(r'id="[^"]*?(\d{4})-(\d{2})-(\d{2})"')
_BLOCK_TIME = re.compile(r'class="time[^"]*"[^>]*>\s*(\d{1,2}):(\d{2})')
_SOLD_OUT = re.compile(r"wyprzedane", re.I)
_SLUG_DT = re.compile(r"-(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})(?:-\d+)?$")
_REPERTOIRE_ID = re.compile(r"currentRepertoireId\s*=\s*(\d+)")
_LOCATION = re.compile(r"Lokalizacja spektaklu:\s*([^<\n]+)")


def _text(s: str) -> str:
    return html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", s))).strip()


def slugify(title: str) -> str:
    s = title.lower().replace("ł", "l")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def parse_programme(body: dict) -> tuple[list[Performance], int]:
    """JSON programu -> predstavenia a počet blokov, ktoré sa nepodarilo spracovať.

    Blok s odkazom na pokladňu je v predaji (id = slug pokladne). Blok, ktorého tlačidlo hovorí
    "wyprzedane", odkaz nemá: stane sa vypredaným predstavením s odvodeným id; stabilné
    `data-instance-tickets` umožní repozitáru neskôr priradiť ho k tomu istému predstaveniu.
    """
    out: dict[str, Performance] = {}
    skipped = 0
    for block in _BLOCK_SPLIT.split(body.get("template", ""))[1:]:
        title = _TITLE.search(block)
        if title is None:
            skipped += 1
            continue
        name = _text(title.group(1))
        stage = _STAGE.search(block)
        stage_name = _text(stage.group(1)).split(" - ")[-1] if stage else None
        inst = _INSTANCE.search(block)
        instance_id = int(inst.group(1)) if inst else None
        ticket = _TICKET.search(block)
        if ticket is not None:
            slug = ticket.group(1)
            m = _SLUG_DT.search(slug)
            if m is None:
                skipped += 1
                continue
            y, mo, d, h, mi = (int(x) for x in m.groups())
            perf = Performance(slug, name, stage_name, datetime(y, mo, d, h, mi, tzinfo=TZ),
                               ticket.group(0), "on_sale", instance_id)
        elif _SOLD_OUT.search(block):
            dm, tm = _BLOCK_DATE.search(block), _BLOCK_TIME.search(block)
            if dm is None or tm is None or instance_id is None:
                skipped += 1
                continue
            y, mo, d = (int(x) for x in dm.groups())
            h, mi = int(tm.group(1)), int(tm.group(2))
            pid = f"{slugify(name)}-{y:04d}-{mo:02d}-{d:02d}-{h:02d}-{mi:02d}"
            perf = Performance(pid, name, stage_name, datetime(y, mo, d, h, mi, tzinfo=TZ),
                               None, "sold_out", instance_id)
        else:
            skipped += 1
            continue
        out[perf.performance_id] = perf
    return list(out.values()), skipped


def parse_event_page(page: str) -> tuple[int, str | None] | None:
    m = _REPERTOIRE_ID.search(page)
    if m is None:
        return None
    loc = _LOCATION.search(page)
    return int(m.group(1)), _text(loc.group(1)) if loc else None


def _price(raw) -> Decimal:
    return Decimal(str(raw).replace(" ", "").replace(",", "."))


def parse_seats(body: dict) -> tuple[list[CategorySeats], int, int]:
    """forSale.json -> (kategórie, miesta v sále, miesta v predaji).

    Dostupnosť podľa kategórie je počítadlo v legende; kľúč `sale` pri jednotlivých miestach
    slúži na krížovú kontrolu (nezhoda sa zaloguje, vyhráva legenda).
    """
    loc = body["location"]
    categories = [
        CategorySeats(str(c["name"]).strip(), _price(c["price"]), int(c["places"]))
        for c in loc["legend"]
    ]
    places = [s for s in loc.get("shapes", []) if s.get("isPlace")]
    on_sale = sum(1 for s in places if "sale" in s)
    legend_total = sum(c.seats_available for c in categories)
    if on_sale != legend_total:
        log.warning("nezhoda legendy a miest: %d vs %d", legend_total, on_sale)
    return categories, len(places), on_sale


def detect_sales(
    previous: dict[tuple[str, Decimal], int], current: list[CategorySeats]
) -> list[tuple[CategorySeats, int]]:
    """Rozdiely v počte miest podľa (kategória, cena). Kladné = predané, záporné = vrátené.

    Kategórie, ktoré v predchádzajúcej snímke neboli, sú nový základ a predaj nedávajú.
    """
    out = []
    for c in current:
        before = previous.get((c.name, c.price))
        if before is not None and before != c.seats_available:
            out.append((c, before - c.seats_available))
    return out


@dataclass(frozen=True)
class SnapshotRecord:
    """Snímka dostupnosti jedného predstavenia (pre publikovanie udalosti)."""

    performance_id: str
    observed_at: datetime
    categories: list[CategorySeats]
    fx_rate: Decimal | None
    eur_prices: dict[tuple[str, Decimal], Decimal | None]


@dataclass
class TheaterRun:
    requests: int = 0
    errors: int = 0
    aborted: bool = False
    found: list[Performance] = field(default_factory=list)
    updated: list[Performance] = field(default_factory=list)
    skipped_blocks: int = 0
    snapshots: list[SnapshotRecord] = field(default_factory=list)
    sales: list[Sale] = field(default_factory=list)


class SlowackiCollector:
    def __init__(
        self,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        pause: float = 1.5,
        max_consecutive_errors: int = 5,
        months_ahead: int = 6,
        max_attempts: int = 4,
    ):
        self.client = client or httpx.Client(timeout=30)
        self.sleep, self.now, self.pause = sleep, now, pause
        self.max_errors, self.months_ahead, self.max_attempts = (
            max_consecutive_errors, months_ahead, max_attempts,
        )
        self._consecutive = 0
        self._first = True

    # ---- HTTP -----------------------------------------------------------------------------
    def _request(self, run: TheaterRun, method: str, url: str, **kw) -> httpx.Response | None:
        if run.aborted:
            return None
        if not self._first:
            self.sleep(self.pause)
        self._first = False
        run.requests += 1
        headers = {"User-Agent": USER_AGENT, **kw.pop("headers", {})}
        if method == "GET":
            resp = get_with_retry(
                self.client, url, params=kw.get("params", {}), headers=headers,
                max_attempts=self.max_attempts, sleep=self.sleep,
            )
        else:
            try:
                resp = self.client.post(url, headers=headers, **kw)
            except httpx.TransportError:
                resp = None
        ok = resp is not None and resp.status_code == 200
        self._consecutive = 0 if ok else self._consecutive + 1
        if not ok:
            run.errors += 1
        if self._consecutive >= self.max_errors:
            run.aborted = True
            log.error("%d chýb za sebou, zastavujem (možná blokácia)", self._consecutive)
        return resp if ok else None

    # ---- program ------------------------------------------------------------------------
    def month_starts(self, today: date) -> list[date]:
        first = today
        out = [first]
        y, m = today.year, today.month
        for _ in range(self.months_ahead - 1):
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
            out.append(date(y, m, 1))
        return out

    def discover(self, conn: psycopg.Connection, run: TheaterRun) -> None:
        now = self.now()
        for start in self.month_starts(now.astimezone(TZ).date()):
            data = {
                "filters[0][type]": "type", "filters[0][value]": "current",
                "filters[1][type]": "EventType", "filters[1][value]": "all",
                "filters[2][type]": "Event", "filters[2][value]": "all",
                "filters[3][type]": "search", "filters[3][value]": "",
                "startDate": start.isoformat(),
            }
            resp = self._request(
                run, "POST", LIST_URL, data=data,
                headers={"X-Requested-With": "XMLHttpRequest", "Referer": LIST_REFERER},
            )
            body = None
            if resp is not None:
                try:
                    body = resp.json()
                    if isinstance(body, str):  # obranne: dvojito zakódovaný JSON
                        body = json.loads(body)
                except ValueError:
                    body = None
            ok = isinstance(body, dict) and "template" in body
            params = {"startDate": start.isoformat()}
            fid = log_fetch(conn, SOURCE, LIST_URL, params,
                            resp.status_code if resp is not None else None,
                            body if ok else None, "pending" if ok else "error")
            if not ok:
                conn.commit()
                continue
            performances, skipped = parse_programme(body)
            run.skipped_blocks += skipped
            for p in performances:
                if p.starts_at <= now:
                    continue
                kind, _ = repo.upsert_performance(conn, p, now)
                if kind == "found":
                    run.found.append(p)
                elif kind == "updated":
                    run.updated.append(p)
            set_parse_status(conn, fid, "ok" if performances else "empty")
            conn.commit()
        repo.mark_past(conn, now)
        conn.commit()
        self._resolve_repertoire_ids(conn, run, now)

    def _resolve_repertoire_ids(
        self, conn: psycopg.Connection, run: TheaterRun, now: datetime
    ) -> None:
        for row in repo.performances_without_repertoire(conn, now):
            resp = self._request(run, "GET", row["url"], params={})
            parsed = parse_event_page(resp.text) if resp is not None else None
            log_fetch(conn, SOURCE, row["url"], None,
                      resp.status_code if resp is not None else None,
                      {"repertoire_id": parsed[0], "location": parsed[1]} if parsed else None,
                      "ok" if parsed else "error")
            if parsed:
                repo.set_repertoire(conn, row["performance_id"], *parsed)
            conn.commit()
            if run.aborted:
                return

    # ---- dostupnosť ---------------------------------------------------------------------
    def snapshot(self, conn: psycopg.Connection, run: TheaterRun, limit: int | None = None) -> None:
        now = self.now()
        rate = get_rate(conn, CURRENCY, now.date())
        if rate is None:
            log.warning("kurz %s->EUR nie je k dispozícii, price_eur bude NULL", CURRENCY)
        rows = [r for r in repo.performances_to_snapshot(conn, now) if r["repertoire_id"]]
        for row in rows[:limit]:
            if run.aborted:
                return
            pid = row["performance_id"]
            resp = self._request(
                run, "GET", SEATS_URL, params={"id": row["repertoire_id"]},
                headers={"X-Requested-With": "XMLHttpRequest", "Referer": row["url"]},
            )
            parsed = None
            if resp is not None:
                try:
                    parsed = parse_seats(resp.json())
                except (ValueError, KeyError, TypeError, InvalidOperation):
                    parsed = None
            summary = None
            if parsed:
                cats, total, on_sale = parsed
                summary = {
                    "repertoire_id": row["repertoire_id"], "places_total": total,
                    "places_on_sale": on_sale,
                    "legend": [[c.name, str(c.price), c.seats_available] for c in cats],
                }
            fid = log_fetch(conn, SOURCE, SEATS_URL, {"id": row["repertoire_id"]},
                            resp.status_code if resp is not None else None, summary,
                            "ok" if parsed else "error")
            if not parsed:
                if resp is not None:  # HTTP 200, ale nepoužiteľné telo (ostatné už započítané)
                    run.errors += 1
                conn.commit()
                log.error("%s: mapa miest nie je dostupná", pid)
                continue
            self._store_snapshot(conn, run, pid, now, cats, rate)
            if repo.set_status(conn, pid, "on_sale" if on_sale > 0 else "sold_out"):
                log.info("%s: stav sa zmenil", pid)
            set_parse_status(conn, fid, "ok")
            conn.commit()

    def _store_snapshot(
        self,
        conn: psycopg.Connection,
        run: TheaterRun,
        pid: str,
        now: datetime,
        cats: list[CategorySeats],
        rate: Decimal | None,
    ) -> None:
        eur = {(c.name, c.price): to_eur(c.price, rate) if rate else None for c in cats}
        previous = repo.previous_snapshot(conn, pid)
        repo.insert_snapshot(conn, pid, now, cats, CURRENCY, rate, eur)
        run.snapshots.append(SnapshotRecord(pid, now, cats, rate, eur))
        if previous is None:
            return  # prvá snímka je iba základ
        prev_at, prev_cats = previous
        for cat, qty in detect_sales(prev_cats, cats):
            sale = Sale(
                sale_id=uuid.uuid4(), performance_id=pid, category=cat.name, quantity=qty,
                unit_price=cat.price, currency=CURRENCY, unit_price_eur=eur[(cat.name, cat.price)],
                fx_rate=rate, detected_from=prev_at, detected_to=now,
            )
            repo.insert_sale(conn, sale)
            run.sales.append(sale)

    def run(
        self, conn: psycopg.Connection, discover: bool = True, snapshot: bool = True,
        limit: int | None = None,
    ) -> TheaterRun:
        run = TheaterRun()
        if discover:
            self.discover(conn, run)
        if snapshot and not run.aborted:
            self.snapshot(conn, run, limit)
        return run


def main() -> None:
    parser = argparse.ArgumentParser(description="Jedenkrát spustí zberač divadla Słowackiho")
    parser.add_argument("--discover-only", action="store_true")
    parser.add_argument("--snapshot-only", action="store_true")
    parser.add_argument("--limit", type=int, help="nasníma najviac N predstavení")
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--publish", action="store_true", help="odoslať udalosti do Kafky")
    args = parser.parse_args()
    get_settings()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    collector = SlowackiCollector(months_ahead=args.months)
    with connect() as conn:
        run = collector.run(
            conn, discover=not args.snapshot_only, snapshot=not args.discover_only,
            limit=args.limit,
        )
    print(
        f"requests={run.requests} errors={run.errors} aborted={run.aborted} "
        f"found={len(run.found)} updated={len(run.updated)} skipped_blocks={run.skipped_blocks} "
        f"snapshots={len(run.snapshots)} sales={len(run.sales)}"
    )
    if args.publish:
        from krakow_di.events.builders import theater_events
        from krakow_di.events.publisher import publish_events

        with connect() as conn:
            events = theater_events(run, get_settings().producer_id, datetime.now(UTC))
            print("udalosti (uložené, odoslané):", publish_events(conn, events))
    for s in run.sales:
        print(
            f"  predaj {s.performance_id} {s.category}: "
            f"{s.quantity} x {s.unit_price} {s.currency}"
        )


if __name__ == "__main__":
    main()
