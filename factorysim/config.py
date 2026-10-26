"""Simulation parameters that are not part of the factory's reference data.

Reference data (materials, products, prices, stations) lives in ``sql/seed.sql``.
This module holds the knobs that describe *how* a run behaves.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SimConfig:
    days: int = 365
    seed: int = 42
    # Service the decaying line family at the end of every Nth day. None = never.
    maintenance_interval_days: int | None = 30
    maintenance_cost: float = 5_000.0
    # Grade-A yield is flat for this many days after maintenance...
    decay_onset_days: int = 15
    # ...then loses this many percentage points per day, and the lost share becomes scrap.
    decay_rate_pp: float = 1.0
    decay_family: str = "Microprocessor XL"
    opening_cash: float = 100_000.0

    def __post_init__(self) -> None:
        if self.days < 1:
            raise ValueError("days must be >= 1")
        if self.maintenance_interval_days is not None and self.maintenance_interval_days < 1:
            raise ValueError("maintenance_interval_days must be >= 1 or None")
        if self.maintenance_cost < 0:
            raise ValueError("maintenance_cost must be >= 0")
        if self.decay_onset_days < 0:
            raise ValueError("decay_onset_days must be >= 0")
        if self.decay_rate_pp < 0:
            raise ValueError("decay_rate_pp must be >= 0")

    def decay_pp(self, days_since_maintenance: int) -> float:
        """Percentage points of Grade-A yield lost on the Nth day after maintenance (N >= 1)."""
        return max(0, days_since_maintenance - self.decay_onset_days) * self.decay_rate_pp
