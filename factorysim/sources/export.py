"""Export a simulation run as the files five source systems would produce, with defects.

The simulator's SQLite database is the ground truth. This module renders it as the
daily exports a real plant's systems would drop into a landing zone, then damages them
the way real exports get damaged: late and re-sent files, duplicate and corrected rows,
schema drift, mixed time zones, inconsistent codes, text-formatted money, truncated
lines and a few genuine business errors.

Every injected defect is written to a manifest (``defects.jsonl``) so the pipeline's
output can be scored. The pipeline itself never reads the manifest or the truth database.

Landing layout: ``<landing>/<source>/<YYYY>/<MM>/<DD>/<file>``, where the date is the
day the file *arrived*, which is not always the business date in its name.
"""

from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

START_DATE = date(2025, 1, 1)  # simulation day 1
PLANT_TZ = ZoneInfo("America/Chicago")
MES_V2_FROM = date(2025, 7, 1)  # MES upgrade: new column names, UTC timestamps
SHIFT_START_HOURS = (6, 14, 22)  # shifts A/B/C (1/2/3 after the MES upgrade), 8 hours each

STATION_CODES = {1: "LITHO-A", 2: "LITHO-B", 3: "PLATE-A", 4: "PLATE-B", 5: "CHEM-A", 6: "CHEM-B"}
STATION_LEGACY = {
    "LITHO-A": "LITH-A",
    "LITHO-B": "LITH-B",
    "PLATE-A": "EPC-A",
    "PLATE-B": "EPC-B",
    "CHEM-A": "CTRK-A",
    "CHEM-B": "CTRK-B",
}
SKU_LEGACY = {
    "MPU-XL-A": "MPUXL-A",
    "MPU-XL-B": "MPUXL-B",
    "MPU-XL-SCRAP": "MPUXL-SCR",
    "MESH-AIM": "AIM-MESH",
    "MESH-SCRAP": "MESH-SCR",
    "SUB-PE": "PE-SUB",
    "SUB-SCRAP": "SUB-SCR",
}
SKU_TYPOS = {
    "MPU-XL-A": "MPU-XL-AA",
    "MPU-XL-B": "MPU-LX-B",
    "MESH-AIM": "MESH-AIN",
    "SUB-PE": "SUB-EP",
}
MATERIAL_CODES = {1: "SI-WFR-300", 2: "CU-ANODE", 3: "PR-POLY"}
WAFERS_PER_BOX = 25
VENDORS = {1: "Siltronix Wafer Co", 2: "Cuprum Anode Ltd", 3: "Müller Photochemie GmbH"}
SERVICE_VENDOR = "Northbay Litho Services"
CUSTOMERS = ("C1001", "C1002", "C1003", "C1004")
OPERATORS = ("op114", "op207", "op233", "op305", "op412")
COMMENTS = (
    "",
    "",
    "",
    "",
    "Lot passed AOI",
    'Recipe "XL-7" re-run, see log',
    "Chamber purge; resumed",
    "Shift note:\nreticle swapped at mid-shift",
    "QC hold released – OK",
)

SOURCES = (
    "mes_production",
    "mes_material_issues",
    "erp_invoices",
    "erp_goods_receipts",
    "erp_stock_snapshots",
    "cmms_work_orders",
    "bank_statements",
)


@dataclass(frozen=True)
class DefectRates:
    """Probability of each defect per eligible file or record."""

    late_file: float = 0.05
    duplicate_file: float = 0.02
    header_repeat: float = 0.02
    duplicate_row: float = 0.005
    corrected_resend: float = 0.003
    code_variant: float = 0.10
    unknown_code: float = 0.003
    malformed_row: float = 0.002
    missing_key: float = 0.004
    uom_box: float = 0.5
    iso_date: float = 0.3
    duplicate_wo_event: float = 0.3
    reversed_invoice: float = 0.01
    double_keyed_line: float = 0.002
    double_payment: float = 0.02  # per received PO: the bank pays it twice
    late_payment: float = 0.02  # per received PO: paid 5-10 days after receipt
    missing_files: int = 1  # MES production files that are never delivered


def business_date(sim_day: int) -> date:
    return START_DATE + timedelta(days=sim_day - 1)


@dataclass
class Record:
    """One logical record, plus the defects to record (and apply) when it is written."""

    data: dict
    defects: list[dict] = field(default_factory=list)


@dataclass
class SourceFile:
    source: str
    name: str
    business_date: date
    records: list[Record]
    arrival_delay: int = 0
    defects: list[dict] = field(default_factory=list)


@dataclass(frozen=True)
class ExportSummary:
    files_written: int
    records_written: int
    defects: dict[str, int]


class _Truth:
    """Ground-truth tables pulled from the simulator database."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.skus = dict(conn.execute("SELECT product_id, sku FROM products"))
        self.prices = dict(conn.execute("SELECT product_id, unit_price FROM products"))
        self.unit_costs = dict(conn.execute("SELECT material_id, unit_cost FROM raw_materials"))
        self.days = conn.execute("SELECT MAX(sim_day) FROM production_runs").fetchone()[0] or 0
        self.runs = conn.execute(
            """SELECT run_id, sim_day, station_id, material_id, units_consumed
               FROM production_runs ORDER BY sim_day, station_id"""
        ).fetchall()
        self.outputs: dict[int, dict[int, int]] = defaultdict(dict)
        for run_id, product_id, units in conn.execute(
            "SELECT run_id, product_id, units FROM production_output ORDER BY run_id, product_id"
        ):
            self.outputs[run_id][product_id] = units
        self.sales: dict[int, list[tuple[int, int, float]]] = defaultdict(list)
        for day, product_id, units, price in conn.execute(
            """SELECT sim_day, product_id, units_sold, unit_price FROM sales_ledger
               ORDER BY sim_day, product_id"""
        ):
            self.sales[day].append((product_id, units, price))
        self.receipts = conn.execute(
            """SELECT po_id, material_id, quantity, unit_cost, received_day FROM purchase_orders
               WHERE received_day IS NOT NULL ORDER BY received_day, po_id"""
        ).fetchall()
        self.maintenance = conn.execute(
            "SELECT event_id, sim_day, cost FROM maintenance_events ORDER BY sim_day"
        ).fetchall()
        self.cash = conn.execute(
            """SELECT sim_day, transaction_type, amount, balance_after, reference
               FROM financial_ledger ORDER BY transaction_id"""
        ).fetchall()
        self.stock_by_day: dict[int, dict[int, int]] = {}
        stock: dict[int, int] = defaultdict(int)
        movements = defaultdict(list)
        for day, material_id, qty in conn.execute(
            "SELECT sim_day, material_id, quantity FROM inventory_movements ORDER BY movement_id"
        ):
            movements[day].append((material_id, qty))
        for day in range(0, self.days + 1):
            for material_id, qty in movements.get(day, []):
                stock[material_id] += qty
            self.stock_by_day[day] = dict(stock)


class _Exporter:
    def __init__(self, truth: _Truth, seed: int, rates: DefectRates) -> None:
        self.truth = truth
        self.rng = np.random.default_rng(seed)
        self.rates = rates
        self.files: list[SourceFile] = []
        self.resends: list[tuple[SourceFile, int]] = []

    # ------------------------------------------------------------------ helpers

    def chance(self, p: float) -> bool:
        return bool(self.rng.random() < p)

    def pick(self, options):
        return options[int(self.rng.integers(len(options)))]

    def local_time(self, day: date, start_hour: int, window_minutes: int) -> datetime:
        """A random wall-clock time in the plant that exists exactly once (no DST gaps)."""
        while True:
            minutes = int(self.rng.integers(window_minutes))
            local = datetime.combine(day, time(start_hour)) + timedelta(minutes=minutes)
            local = local.replace(second=int(self.rng.integers(60)))
            utc = local.replace(tzinfo=PLANT_TZ).astimezone(timezone.utc)
            if utc.astimezone(PLANT_TZ).replace(tzinfo=None) == local:
                return local

    def code_variant(self, code: str, legacy: dict[str, str]) -> tuple[str, list[dict]]:
        if not self.chance(self.rates.code_variant):
            return code, []
        style = self.pick(("lower", "padded", "legacy"))
        variant = {"lower": code.lower(), "padded": f" {code} ", "legacy": legacy[code]}[style]
        return variant, [{"defect": "code_variant", "canonical": code, "style": style}]

    def add(self, source: str, name: str, day: date, records: list[Record]) -> SourceFile:
        f = SourceFile(source, name, day, records)
        self.files.append(f)
        return f

    # ------------------------------------------------------------------ MES

    def build_mes(self) -> None:
        production: dict[int, list[Record]] = defaultdict(list)
        issues: dict[int, list[Record]] = defaultdict(list)
        event_seq = issue_seq = 0
        for run_id, day, station_id, material_id, consumed in self.truth.runs:
            bdate = business_date(day)
            station = STATION_CODES[station_id]
            product_ids = sorted(self.truth.outputs[run_id])
            remaining = np.array([self.truth.outputs[run_id][p] for p in product_ids])
            per_shift = self.rng.multinomial(consumed, [1 / 3] * 3)
            for shift, shift_units in enumerate(per_shift):
                if shift_units == 0:
                    continue
                alloc = self.rng.multivariate_hypergeometric(remaining, int(shift_units))
                remaining = remaining - alloc
                start = SHIFT_START_HOURS[shift]
                issue_seq += 1
                station_value, station_defects = self.code_variant(station, STATION_LEGACY)
                issues[day].append(
                    Record(
                        {
                            "issue_id": f"I{issue_seq:07d}",
                            "ts_local": self.local_time(bdate, start, 30),
                            "station": station_value,
                            "shift": shift,
                            "material": MATERIAL_CODES[material_id],
                            "qty": int(shift_units),
                            "true_station": station,
                            "business_date": bdate,
                        },
                        station_defects,
                    )
                )
                for product_id, units in zip(product_ids, alloc, strict=True):
                    if units == 0:
                        continue
                    event_seq += 1
                    sku = self.truth.skus[product_id]
                    station_value, station_defects = self.code_variant(station, STATION_LEGACY)
                    sku_value, sku_defects = self.code_variant(sku, SKU_LEGACY)
                    production[day].append(
                        Record(
                            {
                                "event_id": f"P{event_seq:07d}",
                                "revision": 1,
                                "ts_local": self.local_time(bdate, start, 480),
                                "station": station_value,
                                "shift": shift,
                                "lot_id": f"L{bdate:%y%m%d}{station_id}{shift + 1}",
                                "sku": sku_value,
                                "qty": int(units),
                                "operator": self.pick(OPERATORS),
                                "comment": self.pick(COMMENTS),
                                "business_date": bdate,
                                "true_station": station,
                                "true_sku": sku,
                            },
                            station_defects + sku_defects,
                        )
                    )

        for day in sorted(production):
            self._mes_row_defects(production, day)
        for day in sorted(production):
            name = f"mes_production_{business_date(day):%Y%m%d}.csv"
            self.add("mes_production", name, business_date(day), production[day])
        for day in sorted(issues):
            name = f"mes_material_issues_{business_date(day):%Y%m%d}.csv"
            self.add("mes_material_issues", name, business_date(day), issues[day])

    def _mes_row_defects(self, production: dict[int, list[Record]], day: int) -> None:
        records = production[day]
        next_day = production.get(day + 1)
        extra: list[Record] = []
        for rec in list(records):
            if any(d["defect"] != "code_variant" for d in rec.defects):
                continue  # one data defect per record; a code variant can combine with it
            if self.chance(self.rates.duplicate_row):
                copy = Record(dict(rec.data), [{"defect": "duplicate_row"}])
                target = next_day if next_day is not None and self.chance(0.5) else extra
                target.append(copy)
                rec.defects.append({"defect": "duplicate_row_original"})
            elif next_day is not None and self.chance(self.rates.corrected_resend):
                true_qty = rec.data["qty"]
                wrong = max(1, true_qty + self.pick((-3, -2, -1, 1, 2, 3)))
                if wrong == true_qty:
                    continue
                correction = Record(
                    dict(rec.data, revision=2), [{"defect": "corrected_resend", "qty": true_qty}]
                )
                rec.data["qty"] = wrong
                rec.defects.append({"defect": "superseded_revision", "qty": wrong})
                next_day.append(correction)
            elif self.chance(self.rates.malformed_row):
                mode = self.pick(("truncated", "bad_qty"))
                rec.defects.append({"defect": "malformed_row", "mode": mode})
        records.extend(extra)

    # ------------------------------------------------------------------ ERP

    def build_invoices(self) -> None:
        doc_seq = 0
        by_day: dict[int, list[Record]] = defaultdict(list)
        last_sales_day = max(self.truth.sales)
        for day in sorted(self.truth.sales):
            bdate = business_date(day)
            lines = self.truth.sales[day]
            n_customers = int(self.rng.integers(1, 4))
            customers = [
                str(c) for c in self.rng.choice(CUSTOMERS, size=n_customers, replace=False)
            ]
            split = {
                pid: self.rng.multinomial(units, [1 / n_customers] * n_customers)
                for pid, units, _ in lines
            }
            records = by_day[day]
            for i, customer in enumerate(customers):
                doc_lines = [
                    (pid, int(split[pid][i]), price) for pid, _, price in lines if split[pid][i]
                ]
                if not doc_lines:
                    continue
                doc_seq += 1
                doc_no = f"INV-25-{doc_seq:06d}"
                iso = self.chance(self.rates.iso_date)
                doc_records = []
                for line_no, (pid, qty, price) in enumerate(doc_lines, start=1):
                    sku = self.truth.skus[pid]
                    sku_value, defects = self.code_variant(sku, SKU_LEGACY)
                    doc_records.append(
                        Record(
                            {
                                "doc_no": doc_no,
                                "doc_type": "INV",
                                "ref_doc": "",
                                "doc_date": bdate,
                                "iso_date": iso,
                                "customer": customer,
                                "line_no": line_no,
                                "sku": sku_value,
                                "qty": qty,
                                "price": price,
                                "true_sku": sku,
                            },
                            defects,
                        )
                    )
                records.extend(doc_records)
                if self.chance(self.rates.reversed_invoice):
                    doc_seq += 1
                    dup_no = f"INV-25-{doc_seq:06d}"
                    doc_seq += 1
                    cm_no = f"CM-25-{doc_seq:06d}"
                    # The duplicate is reversed by a credit memo issued 0-10 days later.
                    cm_day = min(day + int(self.rng.integers(0, 11)), last_sales_day)
                    for r in doc_records:
                        records.append(
                            Record(
                                dict(r.data, doc_no=dup_no),
                                [{"defect": "reversed_invoice", "role": "duplicate"}],
                            )
                        )
                        by_day[cm_day].append(
                            Record(
                                dict(
                                    r.data,
                                    doc_no=cm_no,
                                    doc_type="CM",
                                    ref_doc=dup_no,
                                    qty=-r.data["qty"],
                                    doc_date=business_date(cm_day),
                                ),
                                [
                                    {
                                        "defect": "reversed_invoice",
                                        "role": "credit_memo",
                                        "days_later": cm_day - day,
                                    }
                                ],
                            )
                        )
        for day in sorted(by_day):
            bdate = business_date(day)
            records = by_day[day]
            for rec in list(records):
                if any(d["defect"] != "code_variant" for d in rec.defects):
                    continue
                if self.chance(self.rates.double_keyed_line):
                    same_doc = [r for r in records if r.data["doc_no"] == rec.data["doc_no"]]
                    line_no = max(r.data["line_no"] for r in same_doc) + 1
                    records.append(
                        Record(
                            dict(rec.data, line_no=line_no),
                            [{"defect": "double_keyed_line", "sku": rec.data["true_sku"]}],
                        )
                    )
                elif self.chance(self.rates.unknown_code) and rec.data["true_sku"] in SKU_TYPOS:
                    rec.data["sku"] = SKU_TYPOS[rec.data["true_sku"]]
                    rec.defects.append({"defect": "unknown_code", "value": rec.data["sku"]})
                elif self.chance(self.rates.missing_key):
                    rec.defects.append({"defect": "missing_key", "doc_no": rec.data["doc_no"]})
                elif self.chance(self.rates.malformed_row):
                    mode = self.pick(("truncated", "bad_qty"))
                    rec.defects.append({"defect": "malformed_row", "mode": mode})
            self.add("erp_invoices", f"erp_invoices_{bdate:%Y%m%d}.csv", bdate, records)

    def build_receipts(self) -> None:
        by_day: dict[int, list[Record]] = defaultdict(list)
        for po_id, material_id, qty, unit_cost, day in self.truth.receipts:
            data = {
                "gr_no": f"GR-{po_id:06d}",
                "po_no": f"PO-{po_id:06d}",
                "receipt_date": business_date(day),
                "material": MATERIAL_CODES[material_id],
                "qty": qty,
                "uom": "EA",
                "unit_cost": unit_cost,
                "true_qty": qty,
            }
            defects = []
            if material_id == 1 and qty % WAFERS_PER_BOX == 0 and self.chance(self.rates.uom_box):
                data.update(
                    qty=qty // WAFERS_PER_BOX, uom="BX25", unit_cost=unit_cost * WAFERS_PER_BOX
                )
                defects.append({"defect": "uom_box", "qty_ea": qty})
            by_day[day].append(Record(data, defects))
        for day, records in sorted(by_day.items()):
            bdate = business_date(day)
            self.add("erp_goods_receipts", f"erp_goods_receipts_{bdate:%Y%m%d}.csv", bdate, records)

    def build_snapshots(self) -> None:
        for day in range(1, self.truth.days + 1):
            bdate = business_date(day)
            if bdate.weekday() != 6 and day != self.truth.days:  # Sundays and the final day
                continue
            records = [
                Record({"snapshot_date": bdate, "material": MATERIAL_CODES[m], "on_hand": qty})
                for m, qty in sorted(self.truth.stock_by_day[day].items())
            ]
            self.add("erp_stock_snapshots", f"erp_stock_{bdate:%Y%m%d}.csv", bdate, records)

    # ------------------------------------------------------------------ CMMS and bank

    def build_work_orders(self) -> None:
        for event_id, day, cost in self.truth.maintenance:
            bdate = business_date(day)
            wo = f"WO-25-{event_id:04d}"
            opened = self.local_time(bdate, 7, 60)
            completed = self.local_time(bdate, 15, 120)
            base = {"work_order": wo, "asset": "LITHO-BAY", "technician": self.pick(("rk", "jm"))}
            records = [
                Record(dict(base, event="OPENED", ts=opened)),
                Record(dict(base, event="COMPLETED", ts=completed, cost=cost)),
            ]
            if self.chance(self.rates.duplicate_wo_event):
                records.append(
                    Record(
                        dict(base, event="COMPLETED", ts=completed, cost=cost),
                        [{"defect": "duplicate_wo_event", "work_order": wo}],
                    )
                )
            self.add("cmms_work_orders", f"cmms_work_orders_{bdate:%Y%m%d}.jsonl", bdate, records)

    def build_bank(self) -> None:
        """Bank statements from the cash ledger, with payment errors injected.

        The bank is the real world: a double or late payment changes the statement (and
        every running balance after it), not the ERP. Only reconciliation can catch them.
        """
        maintenance_ref = {f"MX-{e}": f"WO-25-{e:04d}" for e, _, _ in self.truth.maintenance}
        material_of_po = {po: m for po, m, *_ in self.truth.receipts}
        last_day = self.truth.days
        events: dict[int, list[dict]] = defaultdict(list)
        opening = 0.0
        check_no = 10_000
        for day, kind, amount, _, ref in self.truth.cash:
            if kind == "Opening Balance":
                opening = amount
                continue
            defects: list[dict] = []
            if kind == "Sales Revenue":
                desc = f"LOCKBOX DEPOSIT {business_date(day):%Y%m%d} CUSTOMER RECEIPTS"
            elif kind == "Material Purchase":
                po_id = int(ref.split("-")[1])
                vendor = VENDORS[material_of_po[po_id]]
                desc = f"ACH DEBIT {vendor.upper()} PO-{po_id:06d}"
                info = {
                    "po": f"PO-{po_id:06d}",
                    "receipt_date": business_date(day),
                    "amount": -amount,
                }
                if day + 12 <= last_day and self.chance(self.rates.late_payment):
                    delay = int(self.rng.integers(5, 11))
                    paid = day + delay
                    events[paid].append(
                        {
                            "desc": desc,
                            "amount": amount,
                            "defects": [
                                {"defect": "late_payment", **info, "paid_date": business_date(paid)}
                            ],
                        }
                    )
                    continue
                if self.chance(self.rates.double_payment):
                    events[day].append(
                        {
                            "desc": desc,
                            "amount": amount,
                            "defects": [{"defect": "double_payment", **info}],
                        }
                    )
            else:
                check_no += 1
                desc = f"CHECK {check_no} {SERVICE_VENDOR.upper()} {maintenance_ref[ref]}"
            events[day].append({"desc": desc, "amount": amount, "defects": defects})
        balance = opening
        for day in range(1, last_day + 1):
            records = []
            if day == 1:
                records.append(
                    Record(
                        {"desc": "BALANCE FORWARD", "amount": None, "balance": balance, "day": 1}
                    )
                )
            for e in events.get(day, []):
                balance += e["amount"]
                records.append(
                    Record(
                        {"desc": e["desc"], "amount": e["amount"], "balance": balance, "day": day},
                        e["defects"],
                    )
                )
            if records:
                bdate = business_date(day)
                self.add("bank_statements", f"bank_statement_{bdate:%Y%m%d}.csv", bdate, records)

    def drop_files(self) -> list[dict]:
        """Never deliver some MES production files.

        Only a day whose records all live in its own file (no corrections or duplicates in
        neighbouring files) is eligible, so the loss is exactly that day's production.
        """
        mes = [f for f in self.files if f.source == "mes_production"]
        elsewhere = {
            r.data["business_date"]
            for f in mes
            for r in f.records
            if r.data["business_date"] != f.business_date
        }
        eligible = [
            f
            for f in mes
            if f.business_date not in elsewhere
            and all(r.data["business_date"] == f.business_date for r in f.records)
        ]
        dropped = []
        for i in sorted(
            self.rng.choice(len(eligible), size=self.rates.missing_files, replace=False)
        ):
            f = eligible[int(i)]
            self.files.remove(f)
            dropped.append(
                {
                    "defect": "missing_file",
                    "source": f.source,
                    "business_date": f.business_date.isoformat(),
                }
            )
        return dropped

    # ------------------------------------------------------------------ file-level defects

    def apply_file_defects(self) -> None:
        resends = []
        for f in self.files:
            if self.chance(self.rates.late_file):
                f.arrival_delay = int(self.rng.integers(1, 5))
                f.defects.append({"defect": "late_file", "days_late": f.arrival_delay})
            if f.source != "cmms_work_orders" and self.chance(self.rates.header_repeat):
                f.defects.append({"defect": "header_repeat"})
            if self.chance(self.rates.duplicate_file):
                resends.append((f, f.arrival_delay + int(self.rng.integers(1, 4))))
        self.resends = resends


# ---------------------------------------------------------------------- serialization


def _money(rng: np.random.Generator, value: float) -> str:
    style = int(rng.integers(4))
    if style == 0:
        return f"${value:,.2f}"
    if style == 1:
        return f"{value:.2f}"
    if style == 2:
        return f"{value:,.2f}"
    return f"{value:g}"


def _utc(local: datetime) -> datetime:
    return local.replace(tzinfo=PLANT_TZ).astimezone(timezone.utc)


def _render_rows(f: SourceFile, rng: np.random.Generator) -> tuple[list[str] | None, list]:
    """Return (header, rows) where each row is (values, record)."""
    rows = []
    if f.source == "mes_production":
        v2 = f.business_date >= MES_V2_FROM
        header = (
            [
                "event_id",
                "revision",
                "event_ts_utc",
                "station_code",
                "shift_code",
                "lot_id",
                "item_code",
                "quantity",
                "uom",
                "operator_id",
                "comment",
            ]
            if v2
            else [
                "event_id",
                "rev",
                "event_time",
                "station",
                "shift",
                "lot",
                "sku",
                "qty",
                "operator",
                "comment",
            ]
        )
        for rec in f.records:
            d = rec.data
            if v2:
                values = [
                    d["event_id"],
                    str(d["revision"]),
                    f"{_utc(d['ts_local']):%Y-%m-%dT%H:%M:%SZ}",
                    d["station"],
                    str(d["shift"] + 1),
                    d["lot_id"],
                    d["sku"],
                    str(d["qty"]),
                    "EA",
                    d["operator"],
                    d["comment"],
                ]
            else:
                values = [
                    d["event_id"],
                    str(d["revision"]),
                    f"{d['ts_local']:%Y-%m-%d %H:%M:%S}",
                    d["station"],
                    "ABC"[d["shift"]],
                    d["lot_id"],
                    d["sku"],
                    str(d["qty"]),
                    d["operator"],
                    d["comment"],
                ]
            rows.append((values, rec))
        return header, rows
    if f.source == "mes_material_issues":
        header = ["issue_id", "issue_time_local", "station", "shift", "material", "qty", "uom"]
        for rec in f.records:
            d = rec.data
            rows.append(
                (
                    [
                        d["issue_id"],
                        f"{d['ts_local']:%Y-%m-%d %H:%M:%S}",
                        d["station"],
                        "ABC"[d["shift"]],
                        d["material"],
                        str(d["qty"]),
                        "EA",
                    ],
                    rec,
                )
            )
        return header, rows
    if f.source == "erp_invoices":
        header = [
            "document_no",
            "doc_type",
            "reference_doc",
            "document_date",
            "customer_id",
            "line_no",
            "sku",
            "quantity",
            "unit_price",
            "line_amount",
            "currency",
        ]
        for rec in f.records:
            d = rec.data
            doc_date = f"{d['doc_date']:%Y-%m-%d}" if d["iso_date"] else f"{d['doc_date']:%m/%d/%Y}"
            missing = any(x["defect"] == "missing_key" for x in rec.defects)
            rows.append(
                (
                    [
                        "" if missing else d["doc_no"],
                        d["doc_type"],
                        d["ref_doc"],
                        doc_date,
                        d["customer"],
                        str(d["line_no"]),
                        d["sku"],
                        str(d["qty"]),
                        _money(rng, d["price"]),
                        _money(rng, d["qty"] * d["price"]),
                        "USD",
                    ],
                    rec,
                )
            )
        return header, rows
    if f.source == "erp_goods_receipts":
        header = [
            "gr_number",
            "po_number",
            "receipt_date",
            "material",
            "quantity",
            "uom",
            "unit_cost",
            "extended_cost",
        ]
        for rec in f.records:
            d = rec.data
            rows.append(
                (
                    [
                        d["gr_no"],
                        d["po_no"],
                        f"{d['receipt_date']:%Y-%m-%d}",
                        d["material"],
                        str(d["qty"]),
                        d["uom"],
                        f"{d['unit_cost']:.2f}",
                        _money(rng, d["qty"] * d["unit_cost"]),
                    ],
                    rec,
                )
            )
        return header, rows
    if f.source == "erp_stock_snapshots":
        header = ["snapshot_date", "material", "on_hand", "uom"]
        for rec in f.records:
            d = rec.data
            rows.append(
                ([f"{d['snapshot_date']:%Y-%m-%d}", d["material"], str(d["on_hand"]), "EA"], rec)
            )
        return header, rows
    if f.source == "bank_statements":
        header = ["Date", "Description", "Debit", "Credit", "Balance"]
        for rec in f.records:
            d = rec.data
            amount = d["amount"]
            debit = f"{-amount:,.2f}" if amount is not None and amount < 0 else ""
            credit = f"{amount:,.2f}" if amount is not None and amount > 0 else ""
            rows.append(
                (
                    [
                        f"{business_date(d['day']):%m/%d/%Y}",
                        d["desc"],
                        debit,
                        credit,
                        f"{d['balance']:,.2f}",
                    ],
                    rec,
                )
            )
        return header, rows
    if f.source == "cmms_work_orders":
        for rec in f.records:
            d = rec.data
            obj = {
                "work_order": d["work_order"],
                "asset": d["asset"],
                "event": d["event"],
                "timestamp": d["ts"].replace(tzinfo=PLANT_TZ).isoformat(),
                "technician": d["technician"],
            }
            if "cost" in d:
                obj["cost"] = {"amount": f"{d['cost']:.2f}", "currency": "USD"}
            rows.append((obj, rec))
        return None, rows
    raise ValueError(f.source)


ENCODINGS = {
    "mes_production": ("utf-8", "\n"),
    "mes_material_issues": ("utf-8", "\n"),
    "erp_invoices": ("utf-8-sig", "\r\n"),
    "erp_goods_receipts": ("utf-8-sig", "\r\n"),
    "erp_stock_snapshots": ("utf-8-sig", "\r\n"),
    "bank_statements": ("cp1252", "\r\n"),
    "cmms_work_orders": ("utf-8", "\n"),
}


def _serialize(f: SourceFile, rng: np.random.Generator) -> tuple[bytes, list[tuple[int, Record]]]:
    """Render a file to bytes; return (bytes, [(record_number, record)]).

    Record numbers count CSV records (or JSONL lines) from 1, header included, which is
    what the pipeline's bronze layer records as ``_record_number``.
    """
    encoding, newline = ENCODINGS[f.source]
    header, rows = _render_rows(f, rng)
    placed: list[tuple[int, Record]] = []
    if header is None:
        lines = [json.dumps(obj, ensure_ascii=False) for obj, _ in rows]
        placed = [(i, rec) for i, (_, rec) in enumerate(rows, start=1)]
        return (newline.join(lines) + newline).encode(encoding), placed

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator=newline)
    writer.writerow(header)
    record_number = 1
    repeat_at = (
        int(rng.integers(1, len(rows)))
        if len(rows) > 1 and any(d["defect"] == "header_repeat" for d in f.defects)
        else None
    )
    for i, (values, rec) in enumerate(rows):
        if i == repeat_at:
            writer.writerow(header)
            record_number += 1
        for d in rec.defects:
            if d["defect"] == "malformed_row":
                qty_col = header.index(next(c for c in header if c in ("qty", "quantity")))
                if d["mode"] == "truncated":
                    values = values[:qty_col]
                else:
                    values = list(values)
                    values[qty_col] = str(rng.choice(["N/A", "", "1O5", "12.5.0"]))
        writer.writerow(values)
        record_number += 1
        placed.append((record_number, rec))
    return buf.getvalue().encode(encoding, errors="strict"), placed


def export_sources(
    truth_conn: sqlite3.Connection,
    landing: Path,
    manifest_path: Path,
    seed: int = 7,
    rates: DefectRates | None = None,
) -> ExportSummary:
    """Write the messy landing zone and the defect manifest for a finished simulation."""
    rates = rates or DefectRates()
    exporter = _Exporter(_Truth(truth_conn), seed, rates)
    exporter.build_mes()
    exporter.build_invoices()
    exporter.build_receipts()
    exporter.build_snapshots()
    exporter.build_work_orders()
    exporter.build_bank()
    dropped = exporter.drop_files()
    exporter.apply_file_defects()

    manifest: list[dict] = list(dropped)
    written = records = 0

    landed_per_day: dict[date, int] = defaultdict(int)

    def write(f: SourceFile, payload: bytes, arrival: date) -> str:
        path = landing / f.source / f"{arrival:%Y/%m/%d}" / f.name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        # Deterministic landing times: 06:00 UTC on the arrival day, one second apart in
        # the order the files are written (the pipeline orders same-day files by them).
        landed_per_day[arrival] += 1
        stamp = datetime.combine(arrival, time(6), timezone.utc).timestamp()
        ns = int(stamp + landed_per_day[arrival]) * 10**9
        os.utime(path, ns=(ns, ns))
        return path.relative_to(landing).as_posix()

    payloads = {}
    for f in exporter.files:
        payload, placed = _serialize(f, exporter.rng)
        arrival = f.business_date + timedelta(days=f.arrival_delay)
        rel = write(f, payload, arrival)
        payloads[id(f)] = payload
        written += 1
        records += len(placed)
        for d in f.defects:
            manifest.append(
                {**d, "source": f.source, "file": rel, "business_date": f.business_date.isoformat()}
            )
        for record_number, rec in placed:
            for d in rec.defects:
                entry = {
                    **d,
                    "source": f.source,
                    "file": rel,
                    "record_number": record_number,
                    "business_date": f.business_date.isoformat(),
                }
                for key in ("event_id", "issue_id", "doc_no", "line_no", "gr_no", "work_order"):
                    if key in rec.data:
                        entry.setdefault("key_" + key, rec.data[key])
                for key in (
                    "true_station",
                    "true_sku",
                    "qty",
                    "price",
                    "doc_date",
                    "business_date",
                ):
                    if key in rec.data:
                        entry.setdefault("record_" + key, rec.data[key])
                manifest.append(_jsonable(entry))
    for f, delay in exporter.resends:
        arrival = f.business_date + timedelta(days=delay)
        rel = write(f, payloads[id(f)], arrival)
        written += 1
        manifest.append(
            {
                "defect": "duplicate_file",
                "source": f.source,
                "file": rel,
                "business_date": f.business_date.isoformat(),
            }
        )

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as out:
        for entry in manifest:
            out.write(json.dumps(_jsonable(entry), sort_keys=True) + "\n")
    counts: dict[str, int] = defaultdict(int)
    for entry in manifest:
        counts[entry["defect"]] += 1
    return ExportSummary(written, records, dict(sorted(counts.items())))


def _jsonable(entry: dict) -> dict:
    return {k: (v.isoformat() if isinstance(v, date) else v) for k, v in entry.items()}
