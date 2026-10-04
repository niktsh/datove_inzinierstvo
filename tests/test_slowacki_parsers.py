import json
from decimal import Decimal
from pathlib import Path

from krakow_di.collectors.slowacki import (
    detect_sales,
    parse_event_page,
    parse_programme,
    parse_seats,
    slugify,
)
from krakow_di.repo.theater import CategorySeats

FIX = Path(__file__).parent / "fixtures" / "slowacki"


def load(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def test_programme_real_fixture():
    perfs, skipped = parse_programme(load("repertoire_list_2026-11.json"))
    # 4 v predaji + 2 vypredané bloky (bez odkazu na pokladňu); posledný blok fixture je odrezaný
    assert len(perfs) == 6 and skipped == 1
    g = perfs[0]
    assert g.performance_id == "wielki-gatsby-2026-11-03-19-00"
    assert g.title == "Wielki Gatsby" and g.stage == "Scena MOS"
    assert g.url == "https://bilety.teatrwkrakowie.pl/kup-bilet/wielki-gatsby-2026-11-03-19-00"
    assert g.starts_at.isoformat() == "2026-11-03T19:00:00+01:00"
    assert {p.stage for p in perfs} == {"Scena MOS", "Duża Scena"}
    assert len({p.performance_id for p in perfs}) == 6
    assert g.instance_id == 8902 and g.status == "on_sale"
    wesele = [p for p in perfs if p.title == "Wesele"]
    assert [(p.status, p.url, p.instance_id) for p in wesele] == [("sold_out", None, 8904)] + [
        ("sold_out", None, wesele[1].instance_id)]
    assert wesele[0].performance_id == "wesele-2026-11-04-19-00"
    assert wesele[0].starts_at.isoformat() == "2026-11-04T19:00:00+01:00"


def test_slugify_polish():
    assert slugify("Kraków narodowej sztuce, czyli tryumf miernoty") == (
        "krakow-narodowej-sztuce-czyli-tryumf-miernoty")
    assert slugify("Pani Bovary. Możliwa historia") == "pani-bovary-mozliwa-historia"


def test_programme_skips_blocks_without_ticket_link_and_junk():
    body = {"template": '<div class="block" id="x"><h2><a href="/s">Bez kasy</a></h2></div>'
            '<div class="block"><h2><a>Zły slug</a></h2>'
            '<a href="https://bilety.teatrwkrakowie.pl/kup-bilet/bez-daty">k</a></div>'}
    assert parse_programme(body) == ([], 2)
    assert parse_programme({}) == ([], 0)


def test_slug_with_numeric_suffix_keeps_time():
    body = {"template": '<div class="block"><h2><a>Masterclass</a></h2>'
            '<a href="https://bilety.teatrwkrakowie.pl/kup-bilet/masterclass-2026-09-27-10-00-3">k</a>'}
    (p,), _ = parse_programme(body)
    assert p.performance_id.endswith("-3") and p.starts_at.hour == 10 and p.starts_at.minute == 0


def test_event_page():
    page = ("<script>var currentRepertoireId = 1926;</script>"
            " Lokalizacja spektaklu: Scena MOS, ul. Rajska 12 <br>")
    assert parse_event_page(page) == (1926, "Scena MOS, ul. Rajska 12")
    assert parse_event_page("<html>nothing</html>") is None


def test_seats_small_stage():
    cats, total, on_sale = parse_seats(load("for_sale_mos_1926.json"))
    assert (total, on_sale) == (203, 81)
    by = {c.name: c for c in cats}
    assert by["Bilet"].price == Decimal("100.00") and by["Bilet"].seats_available == 81
    assert sum(c.seats_available for c in cats) == on_sale


def test_seats_main_stage_has_two_categories_with_same_name():
    cats, total, on_sale = parse_seats(load("for_sale_duza_scena_1702.json"))
    assert total == 557 and on_sale == 13
    normal = sorted((c.price, c.seats_available) for c in cats if c.name == "Normalny")
    assert normal == [(Decimal("120.00"), 4), (Decimal("150.00"), 0)]


def cs(name, price, n):
    return CategorySeats(name, Decimal(price), n)


def test_detect_sales_decrease_increase_and_new_category():
    prev = {("A", Decimal("100.00")): 10, ("B", Decimal("50.00")): 5, ("C", Decimal("20.00")): 0}
    cur = [cs("A", "100.00", 7), cs("B", "50.00", 6), cs("C", "20.00", 0), cs("D", "9.00", 3)]
    got = {(c.name, q) for c, q in detect_sales(prev, cur)}
    assert got == {("A", 3), ("B", -1)}  # D je základ, C sa nezmenilo


def test_detect_sales_same_name_different_price_are_separate():
    prev = {("Normalny", Decimal("120.00")): 4, ("Normalny", Decimal("150.00")): 2}
    cur = [cs("Normalny", "120.00", 3), cs("Normalny", "150.00", 2)]
    ((c, q),) = detect_sales(prev, cur)
    assert c.price == Decimal("120.00") and q == 1
