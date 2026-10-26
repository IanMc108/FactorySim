"""Daily time-step simulation of the factory, written to the SQLite ledgers.

Each simulated day runs in this order:

1. Receive purchase orders that have arrived (stock in, cash out).
2. Run every station that has enough input material; grade its output.
3. Sell the day's good output at list price.
4. Service the decaying line family if the day is on the maintenance schedule.
5. Place purchase orders for any material whose inventory position is below its reorder point.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import Counter
from dataclasses import dataclass

import numpy as np

from .config import SimConfig
from .db import ReferenceData, Station, load_reference_data

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SimulationSummary:
    days: int
    opening_cash: float
    revenue: float
    material_spend: float
    maintenance_spend: float
    maintenance_events: int
    closing_cash: float
    station_stoppages: int
    units_by_sku: dict[str, int]


def run_simulation(conn: sqlite3.Connection, config: SimConfig) -> SimulationSummary:
    """Simulate ``config.days`` days into a freshly created database."""
    if conn.execute("SELECT COUNT(*) FROM financial_ledger").fetchone()[0]:
        raise ValueError("database already contains a simulation run; create a fresh one")
    ref = load_reference_data(conn)
    with conn:
        _Run(conn, ref, config).execute()
    return summarize(conn, config)


class _Run:
    def __init__(self, conn: sqlite3.Connection, ref: ReferenceData, config: SimConfig) -> None:
        self.conn = conn
        self.ref = ref
        self.config = config
        self.rng = np.random.default_rng(config.seed)
        self.stock = {m.material_id: m.opening_stock for m in ref.materials.values()}
        self.open_orders: list[tuple[int, int, int, int]] = []  # (po_id, material_id, qty, due_day)
        self.cash = 0.0
        self.last_maintenance_day = 0
        self.decay_products = None
        if any(s.family == config.decay_family for s in ref.stations):
            self.decay_products = (
                ref.grade_product(config.decay_family, "A").product_id,
                ref.grade_product(config.decay_family, "Scrap").product_id,
            )

    def execute(self) -> None:
        self._post_cash(0, "Opening Balance", self.config.opening_cash, "OPENING")
        for material_id, qty in self.stock.items():
            self.conn.execute(
                """INSERT INTO inventory_movements (sim_day, material_id, movement_type, quantity)
                   VALUES (0, ?, 'Opening', ?)""",
                (material_id, qty),
            )
        for day in range(1, self.config.days + 1):
            self._receive_orders(day)
            output = self._produce(day)
            self._sell(day, output)
            self._maintain(day)
            self._reorder(day)
            self.conn.executemany(
                "UPDATE raw_materials SET current_stock = ? WHERE material_id = ?",
                [(qty, mid) for mid, qty in self.stock.items()],
            )
            log.debug("day %d closed, cash %.2f", day, self.cash)

    def _post_cash(self, day: int, kind: str, amount: float, reference: str) -> None:
        self.cash += amount
        self.conn.execute(
            """INSERT INTO financial_ledger
                   (sim_day, transaction_type, amount, balance_after, reference)
               VALUES (?, ?, ?, ?, ?)""",
            (day, kind, amount, self.cash, reference),
        )

    def _receive_orders(self, day: int) -> None:
        still_open = []
        for po_id, material_id, qty, due_day in self.open_orders:
            if due_day > day:
                still_open.append((po_id, material_id, qty, due_day))
                continue
            self.stock[material_id] += qty
            self.conn.execute(
                "UPDATE purchase_orders SET received_day = ? WHERE po_id = ?", (day, po_id)
            )
            self.conn.execute(
                """INSERT INTO inventory_movements
                       (sim_day, material_id, movement_type, quantity, po_id)
                   VALUES (?, ?, 'Receipt', ?, ?)""",
                (day, material_id, qty, po_id),
            )
            cost = qty * self.ref.materials[material_id].unit_cost
            self._post_cash(day, "Material Purchase", -cost, f"PO-{po_id}")
        self.open_orders = still_open

    def _yield_probabilities(
        self, station: Station, decay_pp: float
    ) -> tuple[list[int], list[float]]:
        probs = {p.product_id: p.base_yield for p in self.ref.family_products(station.family)}
        if decay_pp and self.decay_products:
            grade_a, scrap = self.decay_products
            shift = min(decay_pp / 100, probs[grade_a])
            probs[grade_a] -= shift
            probs[scrap] += shift
        ids = sorted(probs)
        return ids, [probs[i] for i in ids]

    def _produce(self, day: int) -> Counter[int]:
        output: Counter[int] = Counter()
        days_since = day - self.last_maintenance_day
        for station in self.ref.stations:
            need = station.daily_input_units
            if self.stock[station.material_id] < need:
                self.conn.execute(
                    "INSERT INTO station_stoppages (sim_day, station_id, reason) VALUES (?, ?, ?)",
                    (day, station.station_id, "material shortage"),
                )
                log.info(
                    "day %d: %s starved of material %d", day, station.name, station.material_id
                )
                continue
            self.stock[station.material_id] -= need
            decay = (
                self.config.decay_pp(days_since)
                if station.family == self.config.decay_family
                else 0
            )
            product_ids, probs = self._yield_probabilities(station, decay)
            counts = self.rng.multinomial(need, probs)
            run_id = self.conn.execute(
                """INSERT INTO production_runs
                       (sim_day, station_id, material_id, units_consumed, yield_decay_pp)
                   VALUES (?, ?, ?, ?, ?)""",
                (day, station.station_id, station.material_id, need, decay),
            ).lastrowid
            self.conn.execute(
                """INSERT INTO inventory_movements
                       (sim_day, material_id, movement_type, quantity, run_id)
                   VALUES (?, ?, 'Consumption', ?, ?)""",
                (day, station.material_id, -need, run_id),
            )
            self.conn.executemany(
                "INSERT INTO production_output (run_id, product_id, units) VALUES (?, ?, ?)",
                [(run_id, pid, int(n)) for pid, n in zip(product_ids, counts, strict=True)],
            )
            for pid, n in zip(product_ids, counts, strict=True):
                output[pid] += int(n)
        return output

    def _sell(self, day: int, output: Counter[int]) -> None:
        revenue = 0.0
        for product_id in sorted(output):
            units = output[product_id]
            price = self.ref.products[product_id].unit_price
            if units <= 0 or price <= 0:
                continue
            line_total = units * price
            revenue += line_total
            self.conn.execute(
                """INSERT INTO sales_ledger (sim_day, product_id, units_sold, unit_price, revenue)
                   VALUES (?, ?, ?, ?, ?)""",
                (day, product_id, units, price, line_total),
            )
        if revenue:
            self._post_cash(day, "Sales Revenue", revenue, f"SALES-D{day}")

    def _maintain(self, day: int) -> None:
        interval = self.config.maintenance_interval_days
        if interval is None or day % interval:
            return
        event_id = self.conn.execute(
            "INSERT INTO maintenance_events (sim_day, scope, cost) VALUES (?, ?, ?)",
            (day, self.config.decay_family, self.config.maintenance_cost),
        ).lastrowid
        self._post_cash(day, "Maintenance", -self.config.maintenance_cost, f"MX-{event_id}")
        self.last_maintenance_day = day

    def _reorder(self, day: int) -> None:
        for material in self.ref.materials.values():
            on_order = sum(q for _, mid, q, _ in self.open_orders if mid == material.material_id)
            position = self.stock[material.material_id] + on_order
            while position < material.reorder_point:
                due_day = day + material.lead_time_days
                po_id = self.conn.execute(
                    """INSERT INTO purchase_orders
                           (material_id, quantity, unit_cost, order_day, due_day)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        material.material_id,
                        material.order_quantity,
                        material.unit_cost,
                        day,
                        due_day,
                    ),
                ).lastrowid
                self.open_orders.append(
                    (po_id, material.material_id, material.order_quantity, due_day)
                )
                position += material.order_quantity


def summarize(conn: sqlite3.Connection, config: SimConfig) -> SimulationSummary:
    """Build a run summary from the ledgers (not from in-memory state)."""

    def scalar(sql: str) -> float:
        return conn.execute(sql).fetchone()[0] or 0

    units = dict(
        conn.execute(
            """SELECT p.sku, SUM(o.units) FROM production_output o
               JOIN products p USING (product_id) GROUP BY p.sku ORDER BY p.product_id"""
        ).fetchall()
    )
    return SimulationSummary(
        days=config.days,
        opening_cash=scalar(
            "SELECT amount FROM financial_ledger WHERE transaction_type = 'Opening Balance'"
        ),
        revenue=scalar("SELECT SUM(revenue) FROM sales_ledger"),
        material_spend=-scalar(
            "SELECT SUM(amount) FROM financial_ledger WHERE transaction_type = 'Material Purchase'"
        ),
        maintenance_spend=scalar("SELECT SUM(cost) FROM maintenance_events"),
        maintenance_events=int(scalar("SELECT COUNT(*) FROM maintenance_events")),
        closing_cash=scalar(
            "SELECT balance_after FROM financial_ledger ORDER BY transaction_id DESC LIMIT 1"
        ),
        station_stoppages=int(scalar("SELECT COUNT(*) FROM station_stoppages")),
        units_by_sku=units,
    )
