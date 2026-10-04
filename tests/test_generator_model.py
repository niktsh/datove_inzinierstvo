import random

import pytest

from krakow_di.generator import model
from krakow_di.generator.model import CAPACITIES, ModelParams

P = ModelParams(base_rate=0.004, popularity_sigma=1.0)


def test_time_factor_grows_towards_departure():
    values = [model.time_factor(d) for d in (90, 60, 30, 14, 7, 3, 1, 0)]
    assert values == sorted(values) and values[0] < 0.5 and values[-1] > 2.0
    assert model.time_factor(-5) == model.time_factor(0)  # po odlete sa nezvyšuje


def test_cheaper_than_median_sells_more_and_is_bounded():
    assert model.price_factor(50, 100) > model.price_factor(100, 100) > model.price_factor(200, 100)
    assert model.price_factor(1, 100) == 2.5 and model.price_factor(10_000, 100) == 0.4
    assert model.price_factor(0, 100) == 1.0 and model.price_factor(50, 0) == 1.0


def test_sale_probability_is_a_probability_and_monotonic():
    near = model.sale_probability(P, 1.0, 1, 100, 100)
    far = model.sale_probability(P, 1.0, 60, 100, 100)
    assert 0 < far < near <= 0.5
    assert model.sale_probability(P, 5.0, 1, 100, 100) > near
    assert model.sale_probability(ModelParams(base_rate=0), 1.0, 1, 100, 100) == 0
    huge = model.sale_probability(ModelParams(base_rate=50), 100.0, 0, 1, 100)
    assert huge <= 1.0


def test_probability_scales_with_tick_length():
    one = model.sale_probability(P, 1.0, 5, 100, 100, 300)
    hour = model.sale_probability(P, 1.0, 5, 100, 100, 3600)
    assert one < hour < 1
    assert hour == pytest.approx(1 - (1 - one) ** 12)
    assert model.sale_probability(P, 1.0, 5, 100, 100, 0) == 0


def test_capacity_fill_sizes_and_thresholds():
    rng = random.Random(1)
    assert {model.initial_capacity(rng) for _ in range(200)} <= set(CAPACITIES)
    fills = [model.initial_fill(rng, model.popularity(rng, 1.0), rng.uniform(0, 90))
             for _ in range(500)]
    assert all(0 <= f <= 0.9 for f in fills)
    sizes = {model.sale_size(rng, 10) for _ in range(200)}
    assert sizes == {1, 2, 3} and model.sale_size(rng, 1) == 1
    assert model.crossed_thresholds(0.40, 0.55) == 1
    assert model.crossed_thresholds(0.40, 0.95) == 3
    assert model.crossed_thresholds(0.50, 0.60) == 0  # prah 0,5 už bol prekročený
    assert 1.03 <= model.markup_step(rng) <= 1.12


def test_popularity_is_heavy_tailed_and_deterministic():
    a = [model.popularity(random.Random(f"s|{i}"), 1.0) for i in range(2000)]
    b = [model.popularity(random.Random(f"s|{i}"), 1.0) for i in range(2000)]
    assert a == b and min(a) > 0
    a.sort()
    median, p95 = a[len(a) // 2], a[int(len(a) * 0.95)]
    assert p95 / median > 3  # malá časť ponúk je výrazne obľúbenejšia
