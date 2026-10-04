import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx

from krakow_di.collectors.slowacki import SlowackiCollector

FIX = Path(__file__).parent / "fixtures" / "slowacki"
NOW = datetime(2026, 10, 4, 12, 0, tzinfo=UTC)
PROGRAMME = json.loads((FIX / "repertoire_list_2026-11.json").read_text(encoding="utf-8"))
MOS = json.loads((FIX / "for_sale_mos_1926.json").read_text(encoding="utf-8"))
DUZA = json.loads((FIX / "for_sale_duza_scena_1702.json").read_text(encoding="utf-8"))


class Site:
    """Falošné divadlo: program, stránky udalostí a mapy miest, všetko meniteľné z testu."""

    def __init__(self):
        self.programme = PROGRAMME
        self.requests = []
        self.fail_seats = False
        self.seat_override = None  # ak je nastavené, vráti sa ako odpoveď pre každú mapu miest

    def handler(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        url = str(req.url)
        if req.method == "POST":
            return httpx.Response(200, json=self.programme)
        if "kup-bilet" in url:
            rid = 1000 + abs(hash(url)) % 5000
            return httpx.Response(200, text=(
                f"<script>var currentRepertoireId = {rid};</script>"
                "Lokalizacja spektaklu: Scena MOS, ul. Rajska 12 <br>"))
        if "forSale" in url:
            if self.fail_seats:
                return httpx.Response(500)
            if self.seat_override is not None:
                return httpx.Response(200, json=self.seat_override)
            return httpx.Response(200, json=MOS)
        return httpx.Response(404)


def make(site, **kw):
    client = httpx.Client(transport=httpx.MockTransport(site.handler))
    sleeps = []
    c = SlowackiCollector(client=client, sleep=sleeps.append, now=lambda: NOW,
                          months_ahead=kw.pop("months_ahead", 1), **kw)
    return c, sleeps


def seed_rate(conn):
    conn.execute("INSERT INTO core.fx_rate (rate_date, currency, per_eur) "
                 "VALUES ('2026-10-04', 'PLN', 4.3775)")
    conn.commit()


def with_seats(body, name, delta):
    b = json.loads(json.dumps(body))
    for c in b["location"]["legend"]:
        if c["name"] == name:
            c["places"] += delta
    return b


def test_discover_stores_programme_incl_sold_out_and_repertoire_ids(conn):
    site = Site()
    c, sleeps = make(site)
    run = c.run(conn, snapshot=False)
    assert len(run.found) == 6 and run.skipped_blocks == 1 and run.errors == 0
    rows = conn.execute(
        "SELECT performance_id, status, url, repertoire_id FROM core.theater_performance "
        "ORDER BY performance_id"
    ).fetchall()
    assert len(rows) == 6
    sold = [r for r in rows if r["status"] == "sold_out"]
    assert len(sold) == 2 and all(r["url"] is None and r["repertoire_id"] is None for r in sold)
    on_sale = [r for r in rows if r["status"] == "on_sale"]
    assert len(on_sale) == 4 and all(r["repertoire_id"] for r in on_sale)
    # 1 dopyt na program + 4 stránky udalostí, 1,5 s od seba
    assert run.requests == 5 and sleeps.count(1.5) == 4
    again = c.run(conn, snapshot=False)
    assert again.found == [] and again.requests == 1  # id sa pamätajú


def test_sold_out_performance_keeps_id_when_tickets_return(conn):
    site = Site()
    c, _ = make(site)
    c.run(conn, snapshot=False)
    before = conn.execute(
        "SELECT performance_id FROM core.theater_performance WHERE title='Wesele' "
        "ORDER BY starts_at LIMIT 1").fetchone()["performance_id"]
    inst = conn.execute("SELECT instance_id FROM core.theater_performance "
                        "WHERE performance_id=%s", (before,)).fetchone()["instance_id"]
    # vstupenky sa uvoľnia: prvý vypredaný blok dostane odkaz na pokladňu s *iným* slugom
    tpl = site.programme["template"]
    i = tpl.index(f'data-instance-tickets="{inst}"')
    j = tpl.index("</div>", tpl.index('btn-disabled', i))
    k = tpl.rfind("<div", 0, tpl.index("btn-disabled", i))
    link = ('<a href="https://bilety.teatrwkrakowie.pl/kup-bilet/wesele-2026-11-04-19-00-2" '
            'class="btn btn-tickets">Bilet</a>')
    site.programme = {"template": tpl[:k] + link + tpl[j + len("</div>"):], "templateCalendar": ""}
    run = c.run(conn, snapshot=False)
    row = conn.execute("SELECT performance_id, url FROM core.theater_performance "
                       "WHERE instance_id=%s", (inst,)).fetchone()
    assert row["performance_id"] == before and row["url"].endswith("-2")
    assert conn.execute("SELECT count(*) AS n FROM core.theater_performance").fetchone()["n"] == 6
    assert len(run.updated) >= 1


def test_snapshot_baseline_then_sale_with_eur_price(conn):
    seed_rate(conn)
    site = Site()
    c, _ = make(site)
    first = c.run(conn)
    assert len(first.snapshots) == 4 and first.sales == []  # iba základ
    snap = conn.execute("SELECT * FROM core.theater_snapshot WHERE category='Bilet' LIMIT 1")
    row = snap.fetchone()
    assert row["currency"] == "PLN" and row["price"] == Decimal("100.00")
    assert row["price_eur"] == Decimal("22.84") and row["fx_rate"] == Decimal("4.377500")

    # každé predstavenie má teraz o 3 miesta menej v kategórii "Bilet" (mapa MOS)
    site.seat_override = with_seats(MOS, "Bilet", -3)
    second = c.run(conn, discover=False)
    assert len(second.sales) == 4
    s = second.sales[0]
    assert (s.category, s.quantity) == ("Bilet", 3)
    assert (s.unit_price, s.currency) == (Decimal("100.00"), "PLN")
    assert s.unit_price_eur == Decimal("22.84") and s.fx_rate == Decimal("4.377500")
    assert s.detected_from == NOW and s.detected_to == NOW
    n = conn.execute("SELECT count(*) AS n, sum(quantity) AS q FROM core.theater_sale").fetchone()
    assert (n["n"], n["q"]) == (4, 12)


def test_return_gives_negative_quantity_and_sold_out_status(conn):
    seed_rate(conn)
    site = Site()
    c, _ = make(site)
    c.run(conn)
    empty = json.loads(json.dumps(MOS))
    for cat in empty["location"]["legend"]:
        cat["places"] = 0
    for sh in empty["location"]["shapes"]:
        sh.pop("sale", None)
    site.seat_override = empty
    run = c.run(conn, discover=False)
    assert {s.quantity for s in run.sales} == {81}
    assert conn.execute("SELECT count(*) AS n FROM core.theater_performance "
                        "WHERE status='sold_out' AND url IS NOT NULL").fetchone()["n"] == 4
    back = with_seats(MOS, "Bilet", -80)  # 1 miesto je opäť v predaji (81 - 80)
    site.seat_override = back
    run = c.run(conn, discover=False)
    assert {s.quantity for s in run.sales} == {-1}  # 0 -> 1 miesto: jedno vrátené
    assert conn.execute("SELECT count(*) AS n FROM core.theater_performance "
                        "WHERE status='on_sale'").fetchone()["n"] == 4


def test_no_rate_means_null_eur_but_snapshot_is_kept(conn):
    site = Site()
    c, _ = make(site)
    # v DB nie je kurz a ECB je v tomto teste nedostupná: podmenime vyhľadanie kurzu
    import krakow_di.collectors.slowacki as mod
    orig = mod.get_rate
    mod.get_rate = lambda *a, **k: None
    try:
        run = c.run(conn)
    finally:
        mod.get_rate = orig
    assert len(run.snapshots) == 4
    row = conn.execute("SELECT price_eur, fx_rate FROM core.theater_snapshot LIMIT 1").fetchone()
    assert row == {"price_eur": None, "fx_rate": None}


def test_past_performances_are_marked_and_not_snapshotted(conn):
    seed_rate(conn)
    site = Site()
    c, _ = make(site)
    c.run(conn, snapshot=False)
    conn.execute("UPDATE core.theater_performance SET starts_at = starts_at - interval '60 days'")
    conn.commit()
    run = c.run(conn, discover=False)
    assert run.snapshots == [] and run.requests == 0
    c2, _ = make(site)
    c2.now = lambda: datetime(2026, 12, 1, tzinfo=UTC)
    c2.discover(conn, type(run)())
    assert conn.execute("SELECT count(*) AS n FROM core.theater_performance "
                        "WHERE status='past'").fetchone()["n"] >= 1


def test_blocked_site_aborts_and_logs_errors(conn):
    seed_rate(conn)
    site = Site()
    c, _ = make(site, max_consecutive_errors=2)
    c.run(conn, snapshot=False)
    site.fail_seats = True
    run = c.run(conn, discover=False)
    assert run.aborted and run.errors >= 2 and run.snapshots == []
    assert conn.execute("SELECT count(*) AS n FROM raw.fetch_log "
                        "WHERE parse_status='error'").fetchone()["n"] >= 2


def test_raw_log_stores_compact_seat_summary(conn):
    seed_rate(conn)
    site = Site()
    c, _ = make(site)
    c.run(conn, limit=1)
    row = conn.execute("SELECT payload FROM raw.fetch_log WHERE url LIKE '%forSale%'").fetchone()
    assert row["payload"]["places_total"] == 203 and row["payload"]["places_on_sale"] == 81
    assert ["Bilet", "100.00", 81] in row["payload"]["legend"]
    assert not re.search(r'"shapes"', json.dumps(row["payload"]))
