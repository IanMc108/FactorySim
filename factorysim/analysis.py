"""Expected-value cost model for the maintenance interval, and a simulated cross-check.

The decision being modelled: the lithography lines lose Grade-A yield once they go
``decay_onset_days`` without service. Servicing costs a fixed fee. How often should
the lines be serviced?

For a service interval of T days, each cycle costs the fee plus the expected Grade-A
revenue lost to decay on days ``onset+1 .. T`` of the cycle. The interval that minimises
long-run cost per day is optimal (a standard renewal-reward argument): keep extending the
cycle only while the next day's expected loss is below the cycle's average cost per day.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

import numpy as np

from .config import SimConfig
from .db import ReferenceData, create_database, load_reference_data
from .simulation import run_simulation


@dataclass(frozen=True)
class CostModel:
    maintenance_cost: float
    decay_onset_days: int
    decay_rate_pp: float
    decaying_units_per_day: int  # input units/day on the decaying line family
    base_grade_a_yield: float
    grade_a_margin: float  # value lost when a unit drops from Grade A to scrap
    revenue_per_day_no_decay: float  # expected revenue of the whole plant with no decay

    @classmethod
    def from_reference(cls, ref: ReferenceData, config: SimConfig) -> CostModel:
        grade_a = ref.grade_product(config.decay_family, "A")
        scrap = ref.grade_product(config.decay_family, "Scrap")
        decaying_units = sum(
            s.daily_input_units for s in ref.stations if s.family == config.decay_family
        )
        revenue = sum(
            s.daily_input_units * p.base_yield * p.unit_price
            for s in ref.stations
            for p in ref.family_products(s.family)
        )
        return cls(
            maintenance_cost=config.maintenance_cost,
            decay_onset_days=config.decay_onset_days,
            decay_rate_pp=config.decay_rate_pp,
            decaying_units_per_day=decaying_units,
            base_grade_a_yield=grade_a.base_yield,
            grade_a_margin=grade_a.unit_price - scrap.unit_price,
            revenue_per_day_no_decay=revenue,
        )

    def with_fee(self, fee: float) -> CostModel:
        return replace(self, maintenance_cost=fee)

    @property
    def loss_per_pp_day(self) -> float:
        """Expected revenue lost per percentage point of Grade-A decay, per day."""
        return self.decaying_units_per_day / 100 * self.grade_a_margin

    def daily_loss(self, days_since_maintenance: int) -> float:
        pp = max(0, days_since_maintenance - self.decay_onset_days) * self.decay_rate_pp
        pp = min(pp, self.base_grade_a_yield * 100)
        return pp * self.loss_per_pp_day

    def cycle_cost(self, interval: int) -> float:
        return self.maintenance_cost + sum(self.daily_loss(d) for d in range(1, interval + 1))

    def cost_per_day(self, interval: int) -> float:
        return self.cycle_cost(interval) / interval

    def optimal_interval(self, max_interval: int = 365) -> int:
        return min(range(1, max_interval + 1), key=lambda t: (self.cost_per_day(t), t))

    def horizon_cost(self, interval: int | None, days: int) -> tuple[float, float, int]:
        """Exact expected (decay loss, maintenance spend, events) over a finite horizon.

        Maintenance happens at the end of every ``interval``-th day, matching the simulator.
        """
        loss, events, last = 0.0, 0, 0
        for day in range(1, days + 1):
            loss += self.daily_loss(day - last)
            if interval and day % interval == 0:
                events += 1
                last = day
        return loss, events * self.maintenance_cost, events

    def min_fee_for_interval(self, target: int, max_fee: float = 1e8) -> float:
        """Smallest service fee at which the optimal interval is at least ``target`` days."""
        lo, hi = 0.0, max_fee
        if self.with_fee(hi).optimal_interval() < target:
            raise ValueError(f"no fee up to {max_fee:,.0f} makes {target} days optimal")
        while hi - lo > 1:
            mid = (lo + hi) / 2
            if self.with_fee(mid).optimal_interval() >= target:
                hi = mid
            else:
                lo = mid
        return hi


@dataclass(frozen=True)
class IntervalResult:
    interval: int | None
    events: int
    expected_decay_loss: float
    maintenance_spend: float
    expected_total_cost: float
    simulated_total_cost_mean: float
    simulated_total_cost_std: float


def simulate_interval(base: SimConfig, interval: int | None, seeds: list[int]) -> list[float]:
    """Total maintenance-related cost (fees + revenue shortfall vs. no decay) per seed."""
    costs = []
    for seed in seeds:
        config = replace(base, maintenance_interval_days=interval, seed=seed)
        conn = create_database()
        try:
            model = CostModel.from_reference(load_reference_data(conn), config)
            summary = run_simulation(conn, config)
        finally:
            conn.close()
        shortfall = model.revenue_per_day_no_decay * config.days - summary.revenue
        costs.append(shortfall + summary.maintenance_spend)
    return costs


def compare_intervals(
    base: SimConfig, intervals: list[int | None], seeds: list[int]
) -> list[IntervalResult]:
    model = reference_model(base)
    results = []
    for interval in intervals:
        loss, spend, events = model.horizon_cost(interval, base.days)
        # Offset seeds per interval so each schedule gets independent draws; shared seeds
        # would make every interval's sampling error move together.
        offset = 10_000 * (interval or 0)
        simulated = np.array(simulate_interval(base, interval, [offset + s for s in seeds]))
        results.append(
            IntervalResult(
                interval=interval,
                events=events,
                expected_decay_loss=loss,
                maintenance_spend=spend,
                expected_total_cost=loss + spend,
                simulated_total_cost_mean=float(simulated.mean()),
                simulated_total_cost_std=float(simulated.std(ddof=1)) if len(seeds) > 1 else 0.0,
            )
        )
    return results


def reference_model(config: SimConfig) -> CostModel:
    conn = create_database()
    try:
        return CostModel.from_reference(load_reference_data(conn), config)
    finally:
        conn.close()
