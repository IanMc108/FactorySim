from dataclasses import replace

import pytest

from factorysim.config import SimConfig
from factorysim.db import create_database
from factorysim.simulation import run_simulation


def test_cash_ledger_running_balance_is_consistent(baseline):
    conn, _, summary = baseline
    rows = conn.execute(
        "SELECT amount, balance_after FROM financial_ledger ORDER BY transaction_id"
    ).fetchall()
    balance = 0.0
    for amount, balance_after in rows:
        balance += amount
        assert balance_after == pytest.approx(balance)
    assert summary.closing_cash == pytest.approx(
        summary.opening_cash + summary.revenue - summary.material_spend - summary.maintenance_spend
    )


def test_sales_ledger_matches_cash_ledger_each_day(baseline):
    conn, _, _ = baseline
    sales = dict(conn.execute("SELECT sim_day, SUM(revenue) FROM sales_ledger GROUP BY sim_day"))
    cash = dict(
        conn.execute(
            "SELECT sim_day, amount FROM financial_ledger WHERE transaction_type = 'Sales Revenue'"
        )
    )
    assert sales.keys() == cash.keys()
    assert all(sales[d] == pytest.approx(cash[d]) for d in sales)


def test_only_priced_goods_are_sold(baseline):
    conn, _, _ = baseline
    bad = conn.execute(
        """SELECT COUNT(*) FROM sales_ledger s JOIN products p USING (product_id)
           WHERE p.unit_price = 0 OR s.unit_price != p.unit_price
              OR ABS(s.revenue - s.units_sold * s.unit_price) > 1e-6"""
    ).fetchone()[0]
    assert bad == 0


def test_every_run_outputs_exactly_its_input(baseline):
    conn, _, _ = baseline
    bad = conn.execute(
        """SELECT r.run_id FROM production_runs r JOIN production_output o USING (run_id)
           GROUP BY r.run_id HAVING SUM(o.units) != MAX(r.units_consumed)"""
    ).fetchall()
    assert bad == []


def test_sold_units_equal_good_output(baseline):
    conn, _, _ = baseline
    produced = dict(
        conn.execute(
            """SELECT o.product_id, SUM(o.units) FROM production_output o
               JOIN products p USING (product_id) WHERE p.unit_price > 0 GROUP BY o.product_id"""
        ).fetchall()
    )
    sold = dict(
        conn.execute("SELECT product_id, SUM(units_sold) FROM sales_ledger GROUP BY product_id")
    )
    assert produced == sold


def test_material_is_conserved(baseline):
    conn, _, _ = baseline
    rows = conn.execute(
        """SELECT m.material_id, m.current_stock, SUM(v.quantity),
                  SUM(CASE WHEN v.movement_type = 'Consumption' THEN -v.quantity END),
                  (SELECT SUM(units_consumed) FROM production_runs r
                   WHERE r.material_id = m.material_id)
           FROM raw_materials m JOIN inventory_movements v USING (material_id)
           GROUP BY m.material_id"""
    ).fetchall()
    for _, stock, net_movements, consumed, run_consumption in rows:
        assert stock == net_movements
        assert consumed == run_consumption


def test_received_orders_are_stocked_and_paid_once(baseline):
    conn, _, summary = baseline
    receipts = conn.execute(
        """SELECT COUNT(*), SUM(p.quantity * p.unit_cost) FROM purchase_orders p
           JOIN inventory_movements v ON v.po_id = p.po_id AND v.quantity = p.quantity
           WHERE p.received_day IS NOT NULL AND v.sim_day = p.received_day"""
    ).fetchone()
    received = conn.execute(
        "SELECT COUNT(*) FROM purchase_orders WHERE received_day IS NOT NULL"
    ).fetchone()[0]
    payments = conn.execute(
        """SELECT COUNT(DISTINCT reference), COUNT(*) FROM financial_ledger
           WHERE transaction_type = 'Material Purchase'"""
    ).fetchone()
    assert receipts[0] == received == payments[0] == payments[1]
    assert receipts[1] == pytest.approx(summary.material_spend)


def test_default_run_never_starves(baseline):
    assert baseline[2].station_stoppages == 0


def test_maintenance_follows_schedule(baseline):
    conn, config, _ = baseline
    days = [d for (d,) in conn.execute("SELECT sim_day FROM maintenance_events ORDER BY sim_day")]
    assert days == list(range(30, 361, 30))


def test_recorded_decay_follows_config(baseline):
    conn, config, _ = baseline
    for day, decay in conn.execute(
        """SELECT DISTINCT r.sim_day, r.yield_decay_pp FROM production_runs r
           JOIN production_stations s USING (station_id) WHERE s.product_family = ?""",
        (config.decay_family,),
    ):
        days_since = day - (day - 1) // 30 * 30
        assert decay == config.decay_pp(days_since)


def test_grade_a_output_falls_with_decay(baseline):
    conn, _, _ = baseline
    fresh, worn = conn.execute(
        """SELECT AVG(CASE WHEN r.yield_decay_pp = 0 THEN o.units END),
                  AVG(CASE WHEN r.yield_decay_pp >= 10 THEN o.units END)
           FROM production_output o JOIN production_runs r USING (run_id)
           JOIN products p USING (product_id) WHERE p.sku = 'MPU-XL-A'"""
    ).fetchone()
    assert fresh == pytest.approx(105, abs=2)  # 150 wafers x 70%
    assert worn < fresh - 15


def test_runs_are_deterministic_per_seed():
    def ledger(seed):
        conn = create_database()
        run_simulation(conn, SimConfig(days=60, seed=seed))
        rows = conn.execute(
            "SELECT * FROM production_output ORDER BY run_id, product_id"
        ).fetchall()
        conn.close()
        return rows

    assert ledger(7) == ledger(7)
    assert ledger(7) != ledger(8)


def test_refuses_to_simulate_into_a_used_database(baseline):
    conn, config, _ = baseline
    with pytest.raises(ValueError, match="already contains"):
        run_simulation(conn, config)


def test_shortage_stops_stations_without_negative_stock():
    conn = create_database()
    conn.execute(
        "UPDATE raw_materials SET current_stock = 400, reorder_point = 0 WHERE material_id = 1"
    )
    summary = run_simulation(conn, SimConfig(days=5))
    stoppages = conn.execute(
        "SELECT sim_day, station_id FROM station_stoppages ORDER BY sim_day, station_id"
    ).fetchall()
    # Day 1 runs both lithography lines (400 -> 100); days 2-5 starve both.
    assert stoppages == [(d, s) for d in range(2, 6) for s in (1, 2)]
    assert summary.station_stoppages == 8
    assert conn.execute(
        "SELECT current_stock FROM raw_materials WHERE material_id = 1"
    ).fetchone() == (100,)


def test_no_maintenance_means_unbounded_decay():
    conn = create_database()
    summary = run_simulation(conn, replace(SimConfig(), days=100, maintenance_interval_days=None))
    assert summary.maintenance_events == 0
    late_grade_a = conn.execute(
        """SELECT SUM(o.units) FROM production_output o JOIN production_runs r USING (run_id)
           JOIN products p USING (product_id) WHERE p.sku = 'MPU-XL-A' AND r.sim_day > 90"""
    ).fetchone()[0]
    assert late_grade_a == 0  # 75+ points of decay exceeds the 70% base yield
