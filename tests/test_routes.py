from datetime import date

import pytest

from krakow_di.routes import RoutesConfig, load_routes


def test_real_config_has_at_least_10_origins():
    cfg = load_routes("config/routes.yaml")
    assert cfg.destination == "KRK" and len(cfg.origins) >= 10 and cfg.max_stops == 0


@pytest.mark.parametrize(
    "today,days,expected",
    [
        (date(2026, 10, 3), 90, ["2026-10", "2026-11", "2026-12", "2027-01"]),
        (date(2026, 11, 20), 30, ["2026-11", "2026-12"]),
        (date(2026, 12, 15), 60, ["2026-12", "2027-01", "2027-02"]),
    ],
)
def test_months(today, days, expected):
    cfg = RoutesConfig("KRK", days, "eur", 0, ("BCN",))
    assert cfg.months(today) == expected


def test_duplicate_origin_rejected(tmp_path):
    p = tmp_path / "r.yaml"
    p.write_text("destination: KRK\norigins: [{query: BCN}, {query: bcn}]\n")
    with pytest.raises(ValueError):
        load_routes(p)
