"""Model simulácie predaja leteniek: čisté funkcie bez DB a bez hodín (pozri ARCHITEKTURA.md).

Pravdepodobnosť predaja ponuky v jednom tiku:
    p = base_rate * popularita * f(dni do odletu) * g(cena voči mediánu trasy)
`f` rastie k dátumu odletu, `g` je vyššie pri lacnejšej ponuke. Popularita je náhodná
(lognormálne rozdelenie), takže väčšina ponúk sa predáva málo a malá časť výrazne.
"""

import math
import random
from dataclasses import dataclass

TICK_REFERENCE_SECONDS = 300  # base_rate je pravdepodobnosť na 5-minútový tik
FILL_THRESHOLDS = (0.50, 0.75, 0.90)
MARKUP_RANGE = (0.03, 0.12)  # pri prekročení prahu cena rastie o 3–12 %
CAPACITIES = (180, 186, 189, 195, 215, 230)  # A320/A321/B737
CAPACITY_WEIGHTS = (1, 2, 5, 2, 1, 1)
SALE_SIZES = (1, 2, 3)
SALE_SIZE_WEIGHTS = (0.55, 0.30, 0.15)


@dataclass(frozen=True)
class ModelParams:
    base_rate: float = 0.004
    popularity_sigma: float = 1.0


def popularity(rng: random.Random, sigma: float) -> float:
    return rng.lognormvariate(0.0, sigma)


def time_factor(days_to_departure: float) -> float:
    """f: záujem rastie k odletu (0,25 ďaleko vopred, ~2,25 v deň odletu)."""
    return 0.25 + 2.0 * math.exp(-max(days_to_departure, 0.0) / 25.0)


def price_factor(price: float, route_median: float) -> float:
    """g: lacnejšie než medián trasy sa predáva viac, drahšie menej (obmedzené na 0,4–2,5)."""
    if price <= 0 or route_median <= 0:
        return 1.0
    return min(2.5, max(0.4, route_median / price))


def sale_probability(
    params: ModelParams,
    popularity_: float,
    days_to_departure: float,
    price: float,
    route_median: float,
    dt_seconds: float = TICK_REFERENCE_SECONDS,
) -> float:
    """Pravdepodobnosť aspoň jedného predaja za `dt_seconds` (prepočet na dĺžku tiku)."""
    per_tick = params.base_rate * popularity_ * time_factor(days_to_departure) * price_factor(
        price, route_median
    )
    per_tick = min(per_tick, 0.5)
    return 1.0 - (1.0 - per_tick) ** (dt_seconds / TICK_REFERENCE_SECONDS)


def initial_capacity(rng: random.Random) -> int:
    return rng.choices(CAPACITIES, weights=CAPACITY_WEIGHTS)[0]


def initial_fill(rng: random.Random, popularity_: float, days_to_departure: float) -> float:
    """Počiatočná obsadenosť pri prvom výskyte ponuky (podiel 0–0,9)."""
    near = 0.5 + 1.5 * math.exp(-max(days_to_departure, 0.0) / 25.0)
    return min(0.9, 0.10 * popularity_ * near * rng.uniform(0.5, 1.5))


def sale_size(rng: random.Random, seats_left: int) -> int:
    return min(rng.choices(SALE_SIZES, weights=SALE_SIZE_WEIGHTS)[0], seats_left)


def crossed_thresholds(fill_before: float, fill_after: float) -> int:
    return sum(1 for t in FILL_THRESHOLDS if fill_before < t <= fill_after)


def markup_step(rng: random.Random) -> float:
    return 1.0 + rng.uniform(*MARKUP_RANGE)


# ---- divadlo --------------------------------------------------------------------------------
THEATER_SALE_SIZES = (1, 2, 3, 4)
THEATER_SALE_SIZE_WEIGHTS = (0.40, 0.35, 0.15, 0.10)
THEATER_TIME_SCALE_DAYS = 14.0  # divadelné vstupenky sa kupujú bližšie k termínu než letenky


@dataclass(frozen=True)
class TheaterParams:
    base_rate: float = 0.005  # pravdepodobnosť predaja jedného predstavenia na 5-minútový tik
    popularity_sigma: float = 1.0


def theater_time_factor(days_to_show: float) -> float:
    """Záujem rastie k termínu predstavenia (0,25 ďaleko vopred, ~2,25 v deň predstavenia)."""
    return 0.25 + 2.0 * math.exp(-max(days_to_show, 0.0) / THEATER_TIME_SCALE_DAYS)


def theater_sale_probability(
    params: TheaterParams,
    popularity_: float,
    days_to_show: float,
    dt_seconds: float = TICK_REFERENCE_SECONDS,
) -> float:
    """Pravdepodobnosť aspoň jedného predaja predstavenia za `dt_seconds`."""
    per_tick = min(params.base_rate * popularity_ * theater_time_factor(days_to_show), 0.5)
    return 1.0 - (1.0 - per_tick) ** (dt_seconds / TICK_REFERENCE_SECONDS)


def theater_sale_size(rng: random.Random, seats_left: int) -> int:
    return min(rng.choices(THEATER_SALE_SIZES, weights=THEATER_SALE_SIZE_WEIGHTS)[0], seats_left)
