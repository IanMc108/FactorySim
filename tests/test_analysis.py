from dataclasses import replace

import numpy as np
import pytest

from factorysim.analysis import reference_model, simulate_interval
from factorysim.config import SimConfig


@pytest.fixture(scope="module")
def model():
    return reference_model(SimConfig())


def test_model_constants_come_from_reference_data(model):
    assert model.decaying_units_per_day == 300
    assert model.loss_per_pp_day == 3000  # 300 wafers x 1% x ($1,000 Grade A - $0 scrap)
    # 300 x (0.7 x 1000 + 0.2 x 650) + 40 x 0.9 x 400 + 200 x 0.9 x 250
    assert model.revenue_per_day_no_decay == pytest.approx(308_400)


def test_daily_loss_starts_after_onset_and_caps_at_full_yield(model):
    assert model.daily_loss(15) == 0
    assert model.daily_loss(16) == 3000
    assert model.daily_loss(30) == 45_000
    assert model.daily_loss(500) == 70 * 3000


def test_optimum_is_just_before_decay(model):
    assert model.optimal_interval() == 15


def test_optimum_satisfies_marginal_rule(model):
    for fee in (5_000, 50_000, 120_000, 400_000):
        m = model.with_fee(fee)
        t = m.optimal_interval()
        # Extending one more day would cost more than the cycle's average; stopping a day
        # earlier would leave a cheaper-than-average day unused.
        assert m.daily_loss(t + 1) >= m.cost_per_day(t)
        if t > 1:
            assert m.daily_loss(t) <= m.cost_per_day(t - 1)


def test_fee_thresholds(model):
    assert model.min_fee_for_interval(16) == pytest.approx(45_000, abs=2)
    assert model.min_fee_for_interval(17) == pytest.approx(93_000, abs=2)
    assert model.with_fee(92_000).optimal_interval() == 16
    assert model.with_fee(94_000).optimal_interval() == 17


def test_horizon_cost_counts_scheduled_services(model):
    assert model.horizon_cost(30, 365) == (4_320_000, 60_000, 12)
    assert model.horizon_cost(15, 365) == (0, 120_000, 24)
    loss, spend, events = model.horizon_cost(None, 20)
    assert (spend, events) == (0, 0)
    assert loss == 3000 * (1 + 2 + 3 + 4 + 5)


@pytest.mark.parametrize("interval", [15, 30])
def test_simulation_matches_expected_cost(model, interval):
    seeds = list(range(1000, 1040))
    costs = np.array(simulate_interval(SimConfig(), interval, seeds))
    expected = sum(model.horizon_cost(interval, 365)[:2])
    standard_error = costs.std(ddof=1) / len(costs) ** 0.5
    assert abs(costs.mean() - expected) < 4 * standard_error


def test_config_validation():
    with pytest.raises(ValueError):
        SimConfig(days=0)
    with pytest.raises(ValueError):
        SimConfig(maintenance_interval_days=0)
    with pytest.raises(ValueError):
        replace(SimConfig(), decay_rate_pp=-1)
