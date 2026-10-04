from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import yaml

DEFAULT_PATH = Path("config/routes.yaml")


@dataclass(frozen=True)
class RoutesConfig:
    destination: str
    horizon_days: int
    currency: str
    max_stops: int | None
    origins: tuple[str, ...]

    def months(self, today: date) -> list[str]:
        """Kalendárne mesiace (YYYY-MM) pokrývajúce dnes .. dnes + horizon_days."""
        end = today + timedelta(days=self.horizon_days)
        out, y, m = [], today.year, today.month
        while (y, m) <= (end.year, end.month):
            out.append(f"{y:04d}-{m:02d}")
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        return out


def load_routes(path: Path | str = DEFAULT_PATH) -> RoutesConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    origins = tuple(str(o["query"]).upper() for o in raw["origins"])
    if len(set(origins)) != len(origins):
        raise ValueError("duplicitné letiská odletu v konfigurácii trás")
    if raw["destination"].upper() in origins:
        raise ValueError("cieľ je uvedený medzi letiskami odletu")
    return RoutesConfig(
        destination=raw["destination"].upper(),
        horizon_days=int(raw.get("horizon_days", 90)),
        currency=str(raw.get("currency", "eur")).lower(),
        max_stops=raw.get("max_stops"),
        origins=origins,
    )
