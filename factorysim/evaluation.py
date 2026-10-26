"""Score the ETL pipeline against the simulator's ground truth and the defect manifest.

This is the only module that sees both sides: the truth database and manifest written by
the exporter, and the lake written by the pipeline. The pipeline never reads either.
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

import duckdb

from .sources.export import MATERIAL_CODES, STATION_CODES, business_date

LOSSY_DEFECTS = {"malformed_row", "unknown_code", "missing_key"}
EXPECTED_REASON = {
    ("malformed_row", "truncated"): "malformed_record",
    ("malformed_row", "bad_qty"): "invalid_quantity",
    ("unknown_code", None): "unknown_sku",
    ("missing_key", None): "missing_key",
}


def _load_manifest(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


class _Lake:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.conn = duckdb.connect()

    def rows(self, sql: str) -> list[tuple]:
        return self.conn.execute(sql).fetchall()

    def gold(self, table: str) -> str:
        return f"read_parquet('{self.root / 'gold' / table / '*.parquet'}')"

    def silver(self, entity: str) -> str:
        return f"read_parquet('{self.root / 'silver' / entity / '*' / '*.parquet'}')"

    def quarantine(self) -> str:
        return f"read_parquet('{self.root / 'quarantine' / '*' / '*' / '*.parquet'}')"


def _compare(truth: dict, gold: dict, adjust: dict) -> dict:
    """Compare cell values; differences fully accounted for by ``adjust`` are explained."""
    cells = set(truth) | set(gold)
    exact = explained = 0
    unexplained = []
    for cell in sorted(cells, key=str):
        t, g = truth.get(cell, 0), gold.get(cell, 0)
        if abs(t - g) < 0.005:
            exact += 1
        elif abs(t + adjust.get(cell, 0) - g) < 0.005:
            explained += 1
        else:
            unexplained.append({"cell": [str(c) for c in cell], "truth": t, "gold": g})
    return {
        "cells": len(cells),
        "exact": exact,
        "explained_by_injected_defects": explained,
        "unexplained": len(unexplained),
        "unexplained_examples": unexplained[:5],
    }


def evaluate(truth_db: Path, manifest_path: Path, lake_root: Path) -> dict:
    manifest = _load_manifest(manifest_path)
    lake = _Lake(lake_root)
    truth = sqlite3.connect(f"file:{truth_db}?mode=ro", uri=True)
    skus = dict(truth.execute("SELECT product_id, sku FROM products"))
    by_defect: dict[str, list[dict]] = defaultdict(list)
    for entry in manifest:
        by_defect[entry["defect"]].append(entry)

    def day(d: str | date) -> date:
        return d if isinstance(d, date) else date.fromisoformat(d)

    # ------------------------------------------------------------------ gold vs truth
    truth_production = {
        (business_date(d), STATION_CODES[s], skus[p]): u
        for d, s, p, u in truth.execute(
            """SELECT r.sim_day, r.station_id, o.product_id, o.units
               FROM production_output o JOIN production_runs r USING (run_id) WHERE o.units > 0"""
        )
    }
    gold_production = {
        (d, s, k): u
        for d, s, k, u in lake.rows(
            "SELECT business_date, station_code, sku, units "
            f"FROM {lake.gold('fct_production_daily')}"
        )
    }
    production_adjust: Counter = Counter()
    for e in by_defect["malformed_row"]:
        if e["source"] == "mes_production":
            cell = (day(e["business_date"]), e["record_true_station"], e["record_true_sku"])
            production_adjust[cell] -= e["record_qty"]

    truth_sales_units, truth_sales_revenue = {}, {}
    for d, p, units, revenue in truth.execute(
        "SELECT sim_day, product_id, units_sold, revenue FROM sales_ledger"
    ):
        truth_sales_units[(business_date(d), skus[p])] = units
        truth_sales_revenue[(business_date(d), skus[p])] = revenue
    gold_units, gold_revenue = {}, {}
    for d, k, units, revenue in lake.rows(
        f"SELECT business_date, sku, net_units, net_revenue FROM {lake.gold('fct_sales_daily')}"
    ):
        gold_units[(d, k)] = units
        gold_revenue[(d, k)] = float(revenue)
    unit_adjust: Counter = Counter()
    revenue_adjust: Counter = Counter()
    for defect, sign in (
        ("malformed_row", -1),
        ("unknown_code", -1),
        ("missing_key", -1),
        ("double_keyed_line", 1),
    ):
        for e in by_defect[defect]:
            if e["source"] != "erp_invoices":
                continue
            cell = (day(e["record_doc_date"]), e["record_true_sku"])
            unit_adjust[cell] += sign * e["record_qty"]
            revenue_adjust[cell] += sign * e["record_qty"] * e["record_price"]

    truth_receipts = {
        (f"GR-{po:06d}",): qty
        for po, qty in truth.execute(
            "SELECT po_id, quantity FROM purchase_orders WHERE received_day IS NOT NULL"
        )
    }
    gold_receipts = {
        (g,): q
        for g, q in lake.rows(f"SELECT gr_no, qty_base FROM {lake.gold('fct_goods_receipts')}")
    }

    truth_maintenance = {
        (f"WO-25-{e:04d}", business_date(d)): c
        for e, d, c in truth.execute("SELECT event_id, sim_day, cost FROM maintenance_events")
    }
    gold_maintenance = {
        (w, d): float(c)
        for w, d, c in lake.rows(
            f"SELECT work_order, business_date, cost FROM {lake.gold('fct_maintenance')}"
        )
    }

    truth_balance = {}
    for d, balance in truth.execute(
        "SELECT sim_day, balance_after FROM financial_ledger ORDER BY transaction_id"
    ):
        truth_balance[(business_date(max(d, 1)),)] = balance
    gold_balance = {}
    for d, balance in lake.rows(
        f"SELECT txn_date, balance FROM {lake.gold('fct_bank_transactions')} ORDER BY txn_date"
    ):
        gold_balance[(d,)] = float(balance)

    stock_rows = truth.execute(
        "SELECT sim_day, material_id, quantity FROM inventory_movements ORDER BY movement_id"
    ).fetchall()
    gold_stock = {
        (d, m): q
        for d, m, q in lake.rows(
            f"SELECT snapshot_date, material_code, on_hand FROM {lake.gold('fct_stock_snapshots')}"
        )
    }
    running: Counter = Counter()
    truth_stock = {}
    snapshot_days = {d for d, _ in gold_stock}
    by_day = defaultdict(list)
    for d, m, q in stock_rows:
        by_day[d].append((m, q))
    last_day = max(by_day)
    for d in range(0, last_day + 1):
        for m, q in by_day.get(d, []):
            running[m] += q
        bd = business_date(max(d, 1))
        if d >= 1 and (bd in snapshot_days or bd.weekday() == 6 or d == last_day):
            for m, code in MATERIAL_CODES.items():
                truth_stock[(bd, code)] = running[m]

    gold_vs_truth = {
        "production_units": _compare(truth_production, gold_production, production_adjust),
        "sales_units": _compare(truth_sales_units, gold_units, unit_adjust),
        "sales_revenue": _compare(truth_sales_revenue, gold_revenue, revenue_adjust),
        "goods_receipts_units": _compare(truth_receipts, gold_receipts, {}),
        "maintenance_cost": _compare(truth_maintenance, gold_maintenance, {}),
        "bank_closing_balance": _compare(truth_balance, gold_balance, {}),
        "stock_snapshots": _compare(truth_stock, gold_stock, {}),
    }

    # ------------------------------------------------------------------ quarantine
    quarantined = {
        (f, n): reason
        for f, n, reason in lake.rows(
            f"SELECT _file, _record_number, reject_reason FROM {lake.quarantine()}"
        )
    }
    expected_q = {}
    for defect in LOSSY_DEFECTS:
        for e in by_defect[defect]:
            expected_q[(e["file"], e["record_number"])] = EXPECTED_REASON[(defect, e.get("mode"))]
    hits = set(quarantined) & set(expected_q)
    quarantine = {
        "quarantined": len(quarantined),
        "expected": len(expected_q),
        "true_positives": len(hits),
        "precision": len(hits) / len(quarantined) if quarantined else 1.0,
        "recall": len(hits) / len(expected_q) if expected_q else 1.0,
        "correct_reason": sum(quarantined[k] == expected_q[k] for k in hits),
    }

    # ------------------------------------------------------------------ reconciliation
    recon = lake.rows(
        f"SELECT check_name, business_date, key, status FROM {lake.gold('recon_results')}"
    )
    status = {(c, d, k): s for c, d, k, s in recon}
    exceptions = {(c, d, k) for (c, d, k), s in status.items() if s == "exception"}
    expected_x = set()
    for e in by_defect["double_keyed_line"]:
        d = day(e["record_doc_date"])
        expected_x.add(("production_vs_sales", d, e["record_true_sku"]))
        expected_x.add(("customer_receipts", d, "ALL"))
    flagged = {k for k in expected_x if status.get(k) not in (None, "matched")}
    reconciliation = {
        "exceptions": len(exceptions),
        "expected_business_errors": len(expected_x),
        "business_errors_flagged": len(flagged),
        "flagged_as_exception": len(expected_x & exceptions),
        "masked_by_quarantine": len({k for k in flagged if status[k] == "explained_by_quarantine"}),
        "unexpected_exceptions": sorted([list(map(str, k)) for k in exceptions - expected_x]),
        "status_counts": dict(Counter(status.values())),
    }

    # ------------------------------------------------------------------ per-defect handling
    ingest_log = {
        (f, s)
        for f, s in lake.rows(
            f"SELECT file, status FROM read_parquet('{lake_root}/_control/ingest_log/*.parquet')"
        )
    }
    events = {
        e: (q, rev, station, sku)
        for e, q, rev, station, sku in lake.rows(
            "SELECT event_id, qty, revision, station_code, sku "
            f"FROM {lake.silver('production_events')}"
        )
    }
    event_counts = Counter(
        e for (e,) in lake.rows(f"SELECT event_id FROM {lake.silver('production_events')}")
    )
    issues = {
        i: s
        for i, s in lake.rows(
            f"SELECT issue_id, station_code FROM {lake.silver('material_issues')}"
        )
    }
    lines = {
        (d, n): k
        for d, n, k in lake.rows(f"SELECT doc_no, line_no, sku FROM {lake.silver('invoice_lines')}")
    }
    docs = {d for d, _ in lines}
    receipts = dict(lake.rows(f"SELECT gr_no, qty_base FROM {lake.silver('goods_receipts')}"))
    wo_counts = Counter(
        w for (w,) in lake.rows(f"SELECT work_order FROM {lake.gold('fct_maintenance')}")
    )
    loaded_files = {f for f, s in ingest_log if s == "loaded"}
    quarantined_files = Counter(f for f, _ in quarantined)
    lossy_per_file = Counter(f for f, _ in expected_q)

    def code_variant_ok(e: dict) -> bool:
        canonical = e["canonical"]
        if e["source"] == "mes_production":
            row = events.get(e["key_event_id"])
            return bool(row) and canonical in (row[2], row[3])
        if e["source"] == "mes_material_issues":
            return issues.get(e["key_issue_id"]) == canonical
        return lines.get((e["key_doc_no"], e["key_line_no"])) == canonical

    checks = {
        "duplicate_file": (
            lambda e: (e["file"], "duplicate_content") in ingest_log,
            "re-delivered file skipped by content hash",
        ),
        "late_file": (
            lambda e: e["file"] in loaded_files,
            "loaded in a later batch under its business date",
        ),
        "header_repeat": (
            lambda e: quarantined_files[e["file"]] == lossy_per_file[e["file"]],
            "repeated header dropped, nothing else quarantined",
        ),
        "duplicate_row": (
            lambda e: event_counts[e["key_event_id"]] == 1,
            "one silver row per event id",
        ),
        "corrected_resend": (
            lambda e: events.get(e["key_event_id"], (None, None))[:2] == (e["qty"], 2),
            "revision 2 wins over revision 1",
        ),
        "code_variant": (code_variant_ok, "code mapped to its canonical value"),
        "uom_box": (
            lambda e: receipts.get(e["key_gr_no"]) == e["qty_ea"],
            "boxes of 25 converted to each",
        ),
        "duplicate_wo_event": (
            lambda e: wo_counts[e["work_order"]] == 1,
            "one maintenance fact per work order",
        ),
        "reversed_invoice": (
            lambda e: e["key_doc_no"] in docs,
            "duplicate invoice and its credit memo both kept (net zero)",
        ),
        "malformed_row": (
            lambda e: (e["file"], e["record_number"]) in quarantined,
            "quarantined with a reason",
        ),
        "unknown_code": (
            lambda e: quarantined.get((e["file"], e["record_number"])) == "unknown_sku",
            "quarantined as unknown_sku",
        ),
        "missing_key": (
            lambda e: quarantined.get((e["file"], e["record_number"])) == "missing_key",
            "quarantined as missing_key",
        ),
        "double_keyed_line": (
            lambda e: (
                status.get(("production_vs_sales", day(e["record_doc_date"]), e["record_true_sku"]))
                not in (None, "matched")
            ),
            "not row-detectable; flagged by reconciliation",
        ),
    }
    defects = []
    for name, (check, how) in checks.items():
        entries = by_defect[name]
        handled = sum(bool(check(e)) for e in entries)
        defects.append({"defect": name, "injected": len(entries), "handled": handled, "how": how})

    return {
        "manifest_entries": len(manifest),
        "defects": defects,
        "quarantine": quarantine,
        "reconciliation": reconciliation,
        "gold_vs_truth": gold_vs_truth,
    }
