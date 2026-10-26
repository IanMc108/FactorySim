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
# Errors in the business data itself, which no row-level rule can see.
BUSINESS_ERRORS = ("double_keyed_line", "double_payment", "late_payment", "missing_file")
# Defects that are fixed by mapping through maintained reference data, reported apart from
# the rest because the alias tables are written to match the exporter's legacy codes.
NORMALIZATIONS = ("code_variant",)
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


def _compare(truth: dict, gold: dict, adjust: dict) -> tuple[dict, dict]:
    """Compare cell values; differences fully accounted for by ``adjust`` are explained.

    Returns a JSON-able summary and the verdict for every cell (exact/explained/wrong).
    """
    verdicts = {}
    unexplained = []
    for cell in sorted(set(truth) | set(gold), key=str):
        t, g = truth.get(cell, 0), gold.get(cell, 0)
        if abs(t - g) < 0.005:
            verdicts[cell] = "exact"
        elif abs(t + adjust.get(cell, 0) - g) < 0.005:
            verdicts[cell] = "explained"
        else:
            verdicts[cell] = "wrong"
            unexplained.append({"cell": [str(c) for c in cell], "truth": t, "gold": g})
    counts = Counter(verdicts.values())
    summary = {
        "cells": len(verdicts),
        "exact": counts["exact"],
        "explained_by_injected_defects": counts["explained"],
        "unexplained": counts["wrong"],
        "unexplained_examples": unexplained[:5],
    }
    return summary, verdicts


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
            cell = (day(e["record_business_date"]), e["record_true_station"], e["record_true_sku"])
            production_adjust[cell] -= e["record_qty"]
    missing_days = {day(e["business_date"]) for e in by_defect["missing_file"]}
    for cell, units in truth_production.items():
        if cell[0] in missing_days:
            production_adjust[cell] -= units

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

    truth_issues = {
        (business_date(d), STATION_CODES[s]): u
        for d, s, u in truth.execute(
            "SELECT sim_day, station_id, units_consumed FROM production_runs"
        )
    }
    gold_issues = {
        (d, s): u
        for d, s, u in lake.rows(
            "SELECT business_date, station_code, sum(units)::BIGINT "
            f"FROM {lake.gold('fct_material_issues_daily')} GROUP BY ALL"
        )
    }

    truth_receipts = {
        (business_date(d), f"GR-{po:06d}"): qty
        for po, qty, d in truth.execute(
            """SELECT po_id, quantity, received_day FROM purchase_orders
               WHERE received_day IS NOT NULL"""
        )
    }
    gold_receipts = {
        (d, g): q
        for d, g, q in lake.rows(
            f"SELECT receipt_date, gr_no, qty_base FROM {lake.gold('fct_goods_receipts')}"
        )
    }

    truth_maintenance = {
        (business_date(d), f"WO-25-{e:04d}"): c
        for e, d, c in truth.execute("SELECT event_id, sim_day, cost FROM maintenance_events")
    }
    gold_maintenance = {
        (d, w): float(c)
        for w, d, c in lake.rows(
            f"SELECT work_order, business_date, cost FROM {lake.gold('fct_maintenance')}"
        )
    }

    truth_balance = {}
    for d, balance in truth.execute(
        "SELECT sim_day, balance_after FROM financial_ledger ORDER BY transaction_id"
    ):
        truth_balance[(business_date(max(d, 1)),)] = balance
    # The bank is the real world: an injected double or late payment changes its balance.
    balance_adjust: Counter = Counter()
    for (d,) in truth_balance:
        for e in by_defect["double_payment"]:
            if d >= day(e["receipt_date"]):
                balance_adjust[(d,)] -= e["amount"]
        for e in by_defect["late_payment"]:
            if day(e["receipt_date"]) <= d < day(e["paid_date"]):
                balance_adjust[(d,)] += e["amount"]
    gold_balance = {}
    for d, balance in lake.rows(
        f"SELECT txn_date, balance FROM {lake.gold('fct_bank_transactions')} ORDER BY line_seq"
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

    comparisons = {
        "production_units": _compare(truth_production, gold_production, production_adjust),
        "material_issue_units": _compare(truth_issues, gold_issues, {}),
        "sales_units": _compare(truth_sales_units, gold_units, unit_adjust),
        "sales_revenue": _compare(truth_sales_revenue, gold_revenue, revenue_adjust),
        "goods_receipts_units": _compare(truth_receipts, gold_receipts, {}),
        "maintenance_cost": _compare(truth_maintenance, gold_maintenance, {}),
        "bank_closing_balance": _compare(truth_balance, gold_balance, balance_adjust),
        "stock_snapshots": _compare(truth_stock, gold_stock, {}),
    }
    gold_vs_truth = {name: summary for name, (summary, _) in comparisons.items()}
    verdicts = {name: v for name, (_, v) in comparisons.items()}
    # Every comparison's cells start with a business date.
    cells_by_date: dict[tuple[str, date], list[str]] = defaultdict(list)
    for name, cells in verdicts.items():
        for cell, verdict in cells.items():
            cells_by_date[(name, cell[0])].append(verdict)

    def date_ok(datasets: tuple[str, ...], d: date) -> bool:
        """The datasets have data for ``d`` and none of it is wrong."""
        found = [v for name in datasets for v in cells_by_date.get((name, d), [])]
        return bool(found) and "wrong" not in found

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
    sellable = {sku for _, sku in truth_sales_units}
    stations = sorted(set(STATION_CODES.values()))

    def expected_rows(e: dict) -> set[tuple]:
        """The reconciliation rows an injected business error must show up in."""
        if e["defect"] == "double_keyed_line":
            d = day(e["record_doc_date"])
            return {
                ("production_vs_sales", d, e["record_true_sku"]),
                ("customer_receipts", d, "ALL"),
            }
        if e["defect"] == "double_payment":
            return {("supplier_payments", day(e["receipt_date"]), e["po"])}
        if e["defect"] == "late_payment":
            return {
                ("supplier_payments", day(e["receipt_date"]), e["po"]),
                (
                    "supplier_payments",
                    day(e["paid_date"]),
                    f"{e['po']} (payment outside terms or without receipt)",
                ),
            }
        if e["defect"] == "missing_file":
            d = day(e["business_date"])
            return {("mass_balance", d, s) for s in stations} | {
                ("production_vs_sales", d, sku)
                for sku in sellable
                if truth_sales_units.get((d, sku))
            }
        raise ValueError(e["defect"])

    business_errors = [e for name in BUSINESS_ERRORS for e in by_defect[name]]
    expected_x = (
        set().union(*(expected_rows(e) for e in business_errors)) if business_errors else set()
    )
    # A business error is caught only if it surfaces: as an exception, as a row a person has
    # to review, or still open inside the late-arrival window. "Explained by quarantine"
    # would hide it, and counts as missed.
    surfaced_statuses = ("exception", "needs_review", "open_late_window")
    flagged = {k for k in expected_x if status.get(k) in surfaced_statuses}
    masked = {k for k in expected_x if status.get(k) == "explained_by_quarantine"}
    reconciliation = {
        "exceptions": len(exceptions),
        "expected_business_errors": len(expected_x),
        "business_errors_flagged": len(flagged),
        "flagged_as_exception": len(expected_x & exceptions),
        "flagged_for_review": len({k for k in expected_x if status.get(k) == "needs_review"}),
        "masked_by_quarantine": len(masked),
        "missed": sorted([list(map(str, k)) for k in expected_x - flagged]),
        "unexpected_exceptions": sorted([list(map(str, k)) for k in exceptions - expected_x]),
        "rows_needing_review": sum(s == "needs_review" for s in status.values()),
        "status_counts": dict(Counter(status.values())),
    }

    # ------------------------------------------------------------------ per-defect handling
    ingest_log = {
        (f, s)
        for f, s in lake.rows(
            "SELECT file, status FROM read_parquet("
            f"'{lake_root}/_control/ingest_log/*/*.parquet', hive_partitioning = false)"
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
    receipts = dict(lake.rows(f"SELECT gr_no, qty_base FROM {lake.silver('goods_receipts')}"))
    completions = Counter(
        w
        for (w,) in lake.rows(
            f"SELECT work_order FROM {lake.silver('work_order_events')} WHERE event = 'COMPLETED'"
        )
    )
    doc_types = {
        d: t
        for d, t in lake.rows(
            f"SELECT DISTINCT doc_no, doc_type FROM {lake.silver('invoice_lines')}"
        )
    }
    source_datasets = {
        "mes_production": ("production_units",),
        "mes_material_issues": ("material_issue_units",),
        "erp_invoices": ("sales_units", "sales_revenue"),
        "erp_goods_receipts": ("goods_receipts_units",),
        "erp_stock_snapshots": ("stock_snapshots",),
        "cmms_work_orders": ("maintenance_cost",),
        "bank_statements": ("bank_closing_balance",),
    }

    def production_cell_ok(e: dict) -> bool:
        cell = (day(e["record_business_date"]), e["record_true_station"], e["record_true_sku"])
        return verdicts["production_units"].get(cell) in ("exact", "explained")

    def sales_cell_ok(e: dict) -> bool:
        cell = (day(e["record_doc_date"]), e["record_true_sku"])
        return verdicts["sales_units"].get(cell) in ("exact", "explained") and verdicts[
            "sales_revenue"
        ].get(cell) in ("exact", "explained")

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
            lambda e: (
                (e["file"], "duplicate_content") in ingest_log
                and date_ok(source_datasets[e["source"]], day(e["business_date"]))
            ),
            "re-delivered copy not loaded again; its business date's gold numbers match the truth",
        ),
        "late_file": (
            lambda e: (
                e["file"] in loaded_files
                and date_ok(source_datasets[e["source"]], day(e["business_date"]))
            ),
            "loaded after its business date (a later batch when run incrementally); that "
            "date's gold numbers match the truth",
        ),
        "header_repeat": (
            lambda e: quarantined_files[e["file"]] == lossy_per_file[e["file"]],
            "repeated header dropped, nothing else quarantined",
        ),
        "duplicate_row": (
            lambda e: event_counts[e["key_event_id"]] == 1 and production_cell_ok(e),
            "one silver row per event id; the production total matches the truth",
        ),
        "corrected_resend": (
            lambda e: (
                events.get(e["key_event_id"], (None, None))[:2] == (e["qty"], 2)
                and production_cell_ok(e)
            ),
            "revision 2 wins over revision 1; the production total matches the truth",
        ),
        "code_variant": (
            code_variant_ok,
            "mapped through the alias tables (maintained reference data; a code missing from "
            "them is the unknown_code case)",
        ),
        "uom_box": (
            lambda e: receipts.get(e["key_gr_no"]) == e["qty_ea"],
            "boxes of 25 converted to each",
        ),
        "duplicate_wo_event": (
            lambda e: (
                completions[e["work_order"]] == 1
                and date_ok(("maintenance_cost",), day(e["business_date"]))
            ),
            "one completion event survives; the work order's cost matches the truth",
        ),
        "reversed_invoice": (
            lambda e: (
                doc_types.get(e["key_doc_no"]) == ("CM" if e["role"] == "credit_memo" else "INV")
                and sales_cell_ok(e)
            ),
            "duplicate invoice and credit memo both kept; the day's sales match the truth",
        ),
        "malformed_row": (
            lambda e: (
                quarantined.get((e["file"], e["record_number"]))
                == EXPECTED_REASON[("malformed_row", e["mode"])]
                and (production_cell_ok(e) if e["source"] == "mes_production" else sales_cell_ok(e))
            ),
            "quarantined with the right reason; gold differs from the truth by exactly the lost "
            "record",
        ),
        "unknown_code": (
            lambda e: (
                quarantined.get((e["file"], e["record_number"])) == "unknown_sku"
                and sales_cell_ok(e)
            ),
            "quarantined as unknown_sku, never guessed; gold differs by exactly the lost line",
        ),
        "missing_key": (
            lambda e: (
                quarantined.get((e["file"], e["record_number"])) == "missing_key"
                and sales_cell_ok(e)
            ),
            "quarantined as missing_key; gold differs by exactly the lost line",
        ),
        **{
            name: (
                lambda e: all(status.get(k) in surfaced_statuses for k in expected_rows(e)),
                how,
            )
            for name, how in (
                ("double_keyed_line", "billing error: production vs. sales and deposits flag it"),
                ("double_payment", "PO paid twice: supplier-payment check flags it"),
                ("late_payment", "PO paid outside terms: both the PO and the payment are flagged"),
                ("missing_file", "MES file never arrives: mass balance and sales checks flag it"),
            )
        },
    }
    defects = []
    for name, (check, how) in checks.items():
        entries = by_defect[name]
        handled = sum(bool(check(e)) for e in entries)
        kind = (
            "normalization"
            if name in NORMALIZATIONS
            else "business_error"
            if name in BUSINESS_ERRORS
            else "data"
        )
        defects.append(
            {"defect": name, "kind": kind, "injected": len(entries), "handled": handled, "how": how}
        )

    # With incremental runs, did late files really arrive in a later batch than the day
    # they belong to? (Always 0 for a single run.)
    batches = lake.rows(
        "SELECT file, batch_id, arrival_date, file_business_date FROM read_parquet("
        f"'{lake_root}/_control/ingest_log/*/*.parquet', hive_partitioning = false)"
    )
    first_batch_of_day: dict = {}
    for _, batch, arrival, _ in batches:
        if arrival not in first_batch_of_day or batch < first_batch_of_day[arrival]:
            first_batch_of_day[arrival] = batch
    batch_of = {f: b for f, b, _, _ in batches}
    late_in_later_batch = sum(
        1
        for e in by_defect["late_file"]
        if batch_of.get(e["file"], "") > first_batch_of_day.get(day(e["business_date"]), "~")
    )

    return {
        "manifest_entries": len(manifest),
        "batches": len({b for _, b, _, _ in batches}),
        "late_files_in_later_batch": late_in_later_batch,
        "defects": defects,
        "quarantine": quarantine,
        "reconciliation": reconciliation,
        "gold_vs_truth": gold_vs_truth,
    }
