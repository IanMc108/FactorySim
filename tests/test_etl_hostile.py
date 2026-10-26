"""Hostile inputs outside the exporter's defect catalog.

Each case reproduces a failure found by adversarial review round 1 (docs/review-log.md)
and pins the fixed behaviour. None of these inputs can come from the exporter, so they
check the pipeline's rules rather than its agreement with the generator.
"""

import os
from datetime import date
from pathlib import Path

import duckdb
import pytest

from factorysim.etl.pipeline import run_pipeline

MES_V1 = "event_id,rev,event_time,station,shift,lot,sku,qty,operator,comment\n"
INVOICES = (
    "document_no,doc_type,reference_doc,document_date,customer_id,line_no,sku,quantity,"
    "unit_price,line_amount,currency\n"
)
RECEIPTS = "gr_number,po_number,receipt_date,material,quantity,uom,unit_cost,extended_cost\n"
BANK = "Date,Description,Debit,Credit,Balance\r\n"
# A far-off file that moves the watermark, so nothing below is merely "open".
LATE_SNAPSHOT = (
    "erp_stock_snapshots",
    "2025-06-30",
    "erp_stock_20250630.csv",
    "snapshot_date,material,on_hand,uom\n2025-06-30,PR-POLY,1,EA\n",
)


def land(
    root: Path, source: str, arrival: str, name: str, content: str | bytes, encoding: str = "utf-8"
) -> Path:
    path = root / "landing" / source / arrival.replace("-", "/") / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content if isinstance(content, bytes) else content.encode(encoding))
    return path


def run(root: Path, through: str | None = None, lake: str = "lake"):
    return run_pipeline(
        root / "landing", root / lake, date.fromisoformat(through) if through else None
    )


def rows(root: Path, sql: str, lake: str = "lake") -> list[tuple]:
    return duckdb.connect().execute(sql.replace("@", str(root / lake))).fetchall()


def recon(root: Path, check: str) -> list[tuple]:
    return rows(
        root,
        "SELECT key, expected, actual, status FROM "
        f"read_parquet('@/gold/recon_results/*.parquet') WHERE check_name = '{check}' "
        "ORDER BY ALL",
    )


def test_correction_that_moves_a_record_to_another_month(tmp_path):
    def setup(root: Path) -> None:
        land(
            root,
            "mes_production",
            "2025-05-31",
            "mes_production_20250531.csv",
            MES_V1
            + "P1,1,2025-05-31 13:00:00,LITHO-A,B,L1,MPU-XL-A,10,op1,\n"
            + "P2,1,2025-05-31 14:00:00,LITHO-A,B,L1,MPU-XL-A,7,op1,\n",
        )
        land(
            root,
            "mes_production",
            "2025-06-02",
            "mes_production_20250601.csv",
            MES_V1 + "P1,2,2025-06-01 13:00:00,LITHO-A,B,L1,MPU-XL-A,10,op1,\n",
        )

    setup(tmp_path / "inc")
    run(tmp_path / "inc", "2025-05-31")
    run(tmp_path / "inc")
    setup(tmp_path / "full")
    run(tmp_path / "full")
    sql = (
        "SELECT event_id, revision, business_date FROM "
        "read_parquet('@/silver/production_events/*/*.parquet') ORDER BY ALL"
    )
    expected = [("P1", 2, date(2025, 6, 1)), ("P2", 1, date(2025, 5, 31))]
    assert rows(tmp_path / "inc", sql) == rows(tmp_path / "full", sql) == expected


def test_rejects_from_back_dated_lines_reach_quarantine(tmp_path):
    land(
        tmp_path,
        "erp_invoices",
        "2025-07-01",
        "erp_invoices_20250701.csv",
        INVOICES
        + "INV-7,INV,,06/30/2025,C1,1,SUB-PE,4,250,1000,USD\n"
        + "INV-7,INV,,06/30/2025,C1,2,SUB-XX,4,250,1000,USD\n",
    )
    run(tmp_path)
    assert rows(
        tmp_path, "SELECT reject_reason FROM read_parquet('@/quarantine/*/*/*.parquet')"
    ) == [("unknown_sku",)]


def test_double_and_late_supplier_payments_are_exceptions(tmp_path):
    land(
        tmp_path,
        "erp_goods_receipts",
        "2025-03-04",
        "erp_goods_receipts_20250304.csv",
        RECEIPTS + 'GR-1,PO-1,2025-03-04,SI-WFR-300,60,EA,50.00,"3,000.00"\n'
        'GR-2,PO-2,2025-03-04,PR-POLY,10,EA,100.00,"1,000.00"\n',
    )
    land(
        tmp_path,
        "bank_statements",
        "2025-03-04",
        "bank_statement_20250304.csv",
        BANK + '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
        '03/04/2025,ACH DEBIT SILTRONIX PO-1,"3,000.00",,"97,000.00"\r\n'
        '03/04/2025,ACH DEBIT SILTRONIX PO-1,"3,000.00",,"94,000.00"\r\n',
        "cp1252",
    )
    land(
        tmp_path,
        "bank_statements",
        "2025-04-13",
        "bank_statement_20250413.csv",
        BANK + '04/13/2025,ACH DEBIT MULLER PO-2,"1,000.00",,"93,000.00"\r\n',
        "cp1252",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon(tmp_path, "supplier_payments") == [
        ("PO-1", 3000, 6000, "exception"),  # paid twice
        ("PO-2", 1000, 0, "exception"),  # not paid within terms
        ("PO-2 (payment outside terms or without receipt)", 0, 1000, "exception"),
    ]


def test_quarantine_explains_only_matching_variances(tmp_path):
    land(
        tmp_path,
        "mes_production",
        "2025-03-04",
        "mes_production_20250304.csv",
        MES_V1
        + "P1,1,2025-03-04 13:00:00,CHEM-A,B,L1,SUB-PE,10,op1,\n"
        + "P2,1,2025-03-04 13:00:00,LITHO-A,B,L1,MPU-XL-A,abc,op1,\n",
    )
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,1000010,250,250002500,USD\n",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    # A junk MPU-XL-A row cannot explain a million-unit SUB-PE billing variance.
    assert ("SUB-PE", 10, 1000010, "exception") in recon(tmp_path, "production_vs_sales")


def test_file_version_replaces_all_records_of_the_old_version(tmp_path):
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES
        + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n"
        + "INV-1,INV,,03/04/2025,C1,2,SUB-PE,1,250,250,USD\n",
    )
    run(tmp_path)
    # Overwritten in place: line 1 corrected, line 2 withdrawn.
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,40,250,10000,USD\n",
    )
    summary = run(tmp_path)
    assert summary.files_loaded == 1
    assert rows(
        tmp_path, "SELECT line_no, qty FROM read_parquet('@/silver/invoice_lines/*/*.parquet')"
    ) == [(1, 40)]


def test_newer_extract_wins_even_if_older_file_arrives_later(tmp_path):
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-07",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n",
    )
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-05",
        "erp_invoices_20250305.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,3,250,750,USD\n",
    )
    run(tmp_path)
    assert rows(tmp_path, "SELECT qty FROM read_parquet('@/silver/invoice_lines/*/*.parquet')") == [
        (3,)
    ]


def test_reissued_bank_statement_replaces_the_original(tmp_path):
    for arrival, amount, balance in (
        ("2025-03-04", "5,000.00", "105,000.00"),
        ("2025-03-06", "5,500.00", "105,500.00"),
    ):
        land(
            tmp_path,
            "bank_statements",
            arrival,
            "bank_statement_20250304.csv",
            BANK + '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
            f'03/04/2025,LOCKBOX DEPOSIT C1,,"{amount}","{balance}"\r\n',
            "cp1252",
        )
    run(tmp_path)
    assert rows(
        tmp_path,
        "SELECT amount FROM read_parquet('@/silver/bank_transactions/*/*.parquet') "
        "WHERE txn_kind = 'customer_receipts'",
    ) == [(5500,)]


def test_corrected_work_order_completion_uses_latest_cost(tmp_path):
    land(
        tmp_path,
        "cmms_work_orders",
        "2025-03-04",
        "cmms_work_orders_20250304.jsonl",
        '{"work_order":"WO-1","event":"OPENED","timestamp":"2025-03-04T08:00:00-06:00",'
        '"asset":"LITHO-BAY"}\n'
        '{"work_order":"WO-1","event":"COMPLETED","timestamp":"2025-03-04T12:00:00-06:00",'
        '"asset":"LITHO-BAY","cost":{"amount":"50000.00","currency":"USD"}}\n',
    )
    land(
        tmp_path,
        "cmms_work_orders",
        "2025-03-05",
        "cmms_work_orders_20250305.jsonl",
        '{"work_order":"WO-1","event":"COMPLETED","timestamp":"2025-03-04T12:05:00-06:00",'
        '"asset":"LITHO-BAY","cost":{"amount":"5000.00","currency":"USD"}}\n',
    )
    run(tmp_path)
    assert rows(tmp_path, "SELECT cost FROM read_parquet('@/gold/fct_maintenance/*.parquet')") == [
        (5000,)
    ]


def test_bad_files_are_rejected_without_stopping_the_batch(tmp_path):
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n",
    )
    land(tmp_path, "erp_invoices", "2025-03-04", "_SUCCESS", b"")
    land(tmp_path, "erp_invoices", "2025-03-04", "readme.txt", b"not a feed file")
    land(
        tmp_path,
        "mes_production",
        "2025-03-05",
        "mes_production_20250305.csv",
        b"event_id,rev\nP1,1\x81\n",
    )  # 0x81 is undefined in both UTF-8 and cp1252
    summary = run(tmp_path)
    assert summary.files_loaded == 1
    assert sorted(f for f, _ in summary.rejected_files) == [
        "erp_invoices/2025/03/04/readme.txt",
        "mes_production/2025/03/05/mes_production_20250305.csv",
    ]
    assert rows(
        tmp_path, "SELECT count(*) FROM read_parquet('@/silver/invoice_lines/*/*.parquet')"
    ) == [(1,)]


def test_crash_leftovers_are_cleaned_and_lost_bronze_is_rebuilt(tmp_path):
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n",
    )
    run(tmp_path)
    bronze = next((tmp_path / "lake/bronze/erp_invoices").rglob("*.parquet"))
    # A crash after writing bronze but before logging it leaves an unlogged file behind.
    orphan = bronze.parent / "erp_invoices_20250305__000000000000.parquet"
    orphan.write_bytes(bronze.read_bytes())
    run(tmp_path)
    assert not orphan.exists()
    # A logged bronze file that has gone missing is rebuilt from the landing zone.
    bronze.unlink()
    run(tmp_path)
    assert bronze.exists()
    assert rows(
        tmp_path, "SELECT count(*) FROM read_parquet('@/silver/invoice_lines/*/*.parquet')"
    ) == [(1,)]


def test_numbers_are_parsed_strictly(tmp_path):
    land(
        tmp_path,
        "mes_production",
        "2025-03-04",
        "mes_production_20250304.csv",
        MES_V1
        + "P1,1,2025-03-04 13:00:00,LITHO-A,B,L1,MPU-XL-A,10.5,op1,\n"
        + "P2,1,2025-03-04 13:00:00,LITHO-A,B,L1,MPU-XL-A,1e3,op1,\n"
        + "P3,1,2025-03-04 13:00:00,LITHO-A,B,L1,MPU-XL-A,1_000,op1,\n"
        + "P4,1.0,2025-03-04 13:00:00,LITHO-A,B,L1,MPU-XL-A,5,op1,\n"
        + "P5,1,2025-03-04 13:00:00.250,LITHO-A,B,L1,MPU-XL-A,5,op1,\n"
        + "P6,1,2025-03-04 13:00:00,LITHO-A,B,L1,MPU-XL-A, 7 ,op1,\n",
    )
    run(tmp_path)
    assert rows(
        tmp_path, "SELECT event_id, qty FROM read_parquet('@/silver/production_events/*/*.parquet')"
    ) == [("P6", 7)]
    assert rows(
        tmp_path,
        "SELECT _record_number, reject_reason FROM "
        "read_parquet('@/quarantine/*/*/*.parquet') ORDER BY 1",
    ) == [
        (2, "invalid_quantity"),
        (3, "invalid_quantity"),
        (4, "invalid_quantity"),
        (5, "invalid_revision"),
        (6, "invalid_timestamp"),
    ]


def test_bank_balance_is_checked_line_by_line(tmp_path):
    land(
        tmp_path,
        "bank_statements",
        "2025-03-04",
        "bank_statement_20250304.csv",
        BANK + '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
        '03/04/2025,LOCKBOX DEPOSIT C1,,"5,000.00","999,999.00"\r\n'
        '03/04/2025,LOCKBOX DEPOSIT C2,,"1,000.00","106,000.00"\r\n',
        "cp1252",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    statuses = [s for *_, s in recon(tmp_path, "bank_balance_continuity")]
    assert statuses == ["exception", "exception"]


# ---------------------------------------------------------------------- round 2

MES_V2 = (
    "event_id,revision,event_ts_utc,station_code,shift_code,lot_id,item_code,quantity,uom,"
    "operator_id,comment\n"
)
ISSUES = "issue_id,issue_time_local,station,shift,material,qty,uom\n"


def recon_status(root: Path, check: str, key: str) -> str:
    found = [s for k, _, _, s in recon(root, check) if k == key]
    assert len(found) == 1, (check, key, recon(root, check))
    return found[0]


def silver(root: Path, entity: str, columns: str) -> list[tuple]:
    return rows(
        root, f"SELECT {columns} FROM read_parquet('@/silver/{entity}/*/*.parquet') ORDER BY ALL"
    )


def test_unsized_quarantine_never_explains_and_is_bounded(tmp_path):
    # Two MES rows with unreadable quantities at CHEM-A (20 units issued but not produced),
    # and an invoice that bills a million units more than anything produced.
    land(
        tmp_path,
        "mes_material_issues",
        "2025-03-04",
        "mes_material_issues_20250304.csv",
        ISSUES + "I1,2025-03-04 14:00:00,CHEM-A,B,PR-POLY,30,EA\n",
    )
    land(
        tmp_path,
        "mes_production",
        "2025-03-04",
        "mes_production_20250304.csv",
        MES_V1
        + "P1,1,2025-03-04 15:00:00,CHEM-A,B,L1,SUB-PE,10,op1,\n"
        + "P2,1,2025-03-04 15:00:00,CHEM-A,B,L1,SUB-PE,abc,op1,\n"
        + "P3,1,2025-03-04 15:00:00,CHEM-A,B,L1,SUB-PE,xyz,op1,\n",
    )
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,1000010,250,250002500,USD\n",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    # The lost rows really can explain the mass-balance gap, but nobody can confirm it.
    assert recon_status(tmp_path, "mass_balance", "CHEM-A") == "needs_review"
    # They cannot be more than the 20-unit gap, so the billing variance is an exception.
    assert recon_status(tmp_path, "production_vs_sales", "SUB-PE") == "exception"


def test_unknown_key_quarantine_explains_only_an_exact_match(tmp_path):
    land(
        tmp_path,
        "mes_production",
        "2025-03-04",
        "mes_production_20250304.csv",
        MES_V1 + "P1,1,2025-03-04 15:00:00,CHEM-A,B,L1,SUB-PE,10,op1,\n",
    )
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES
        + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,6,250,1500,USD\n"
        + "INV-1,INV,,03/04/2025,C1,2,SUB-XX,4,250,1000,USD\n",
    )  # unknown SKU, qty 4
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon_status(tmp_path, "production_vs_sales", "SUB-PE") == "explained_by_quarantine"


def test_quarantine_is_released_when_its_key_loads_later(tmp_path):
    land(
        tmp_path,
        "mes_production",
        "2025-03-04",
        "mes_production_20250304.csv",
        MES_V1 + "P1,1,2025-03-04 15:00:00,CHEM-A,B,L1,SUB-PE,30,op1,\n",
    )
    # Line 2 is rejected (its amount is wrong) and re-sent correctly the next day. 30 units
    # were made and only 20 billed: a real 10-unit under-billing that the stale quarantined
    # copy of line 2 (effect -10) must not "explain".
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES
        + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,10,250,2500,USD\n"
        + "INV-1,INV,,03/04/2025,C1,2,SUB-PE,10,250,9999,USD\n",
    )
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-05",
        "erp_invoices_20250305.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,2,SUB-PE,10,250,2500,USD\n",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon_status(tmp_path, "production_vs_sales", "SUB-PE") == "exception"
    assert rows(
        tmp_path,
        "SELECT records, released_by_later_load FROM "
        "read_parquet('@/gold/dq_quarantine_summary/*.parquet')",
    ) == [(1, 1)]


def test_quarantined_invoice_amount_explains_a_deposit_gap_exactly(tmp_path):
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES
        + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n"
        + "INV-1,INV,,03/04/2025,C1,2,SUB-XX,2,500,1000,USD\n",
    )  # unknown SKU
    land(
        tmp_path,
        "bank_statements",
        "2025-03-04",
        "bank_statement_20250304.csv",
        BANK + '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
        '03/04/2025,LOCKBOX DEPOSIT 20250304,,"2,000.00","102,000.00"\r\n',
        "cp1252",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon_status(tmp_path, "customer_receipts", "ALL") == "explained_by_quarantine"


def test_quarantined_record_is_dated_by_its_own_date(tmp_path):
    # 10 made and nothing billed for 07/01. A lost invoice line would fit, but the only
    # junk line in the 07/01 file is dated 06/30, so it cannot account for it.
    land(
        tmp_path,
        "mes_production",
        "2025-07-01",
        "mes_production_20250701.csv",
        MES_V2 + "P1,1,2025-07-01T18:00:00Z,CHEM-A,2,L1,SUB-PE,10,EA,op1,\n",
    )
    land(
        tmp_path,
        "erp_invoices",
        "2025-07-01",
        "erp_invoices_20250701.csv",
        INVOICES + "INV-0,INV,,06/30/2025,C1,1,SUB-PE,zz,250,250,USD\n",
    )
    land(
        tmp_path,
        "erp_stock_snapshots",
        "2025-09-30",
        "erp_stock_20250930.csv",
        "snapshot_date,material,on_hand,uom\n2025-09-30,PR-POLY,1,EA\n",
    )
    run(tmp_path)
    assert recon_status(tmp_path, "production_vs_sales", "SUB-PE") == "exception"


def test_partial_deliveries_and_payment_terms_boundary(tmp_path):
    land(
        tmp_path,
        "erp_goods_receipts",
        "2025-03-04",
        "erp_goods_receipts_20250304.csv",
        RECEIPTS + "GR-1,PO-1,2025-03-04,PR-POLY,10,EA,100.00,1000.00\n"
        "GR-3,PO-3,2025-03-04,PR-POLY,10,EA,100.00,1000.00\n"
        "GR-4,PO-4,2025-03-04,PR-POLY,10,EA,100.00,1000.00\n",
    )
    land(
        tmp_path,
        "erp_goods_receipts",
        "2025-03-06",
        "erp_goods_receipts_20250306.csv",
        RECEIPTS + "GR-2,PO-1,2025-03-06,PR-POLY,10,EA,100.00,1000.00\n",
    )
    land(
        tmp_path,
        "bank_statements",
        "2025-03-04",
        "bank_statement_20250304.csv",
        BANK + '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
        '03/04/2025,ACH DEBIT X PO-1,"1,000.00",,"99,000.00"\r\n',
        "cp1252",
    )
    land(
        tmp_path,
        "bank_statements",
        "2025-03-07",
        "bank_statement_20250307.csv",
        BANK + '03/07/2025,ACH DEBIT X PO-1,"1,000.00",,"98,000.00"\r\n'
        '03/07/2025,ACH DEBIT X PO-3,"1,000.00",,"97,000.00"\r\n',
        "cp1252",
    )  # day +3
    land(
        tmp_path,
        "bank_statements",
        "2025-03-08",
        "bank_statement_20250308.csv",
        BANK + '03/08/2025,ACH DEBIT X PO-4,"1,000.00",,"96,000.00"\r\n',
        "cp1252",
    )  # day +4
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon_status(tmp_path, "supplier_payments", "PO-1") == "matched"  # two deliveries
    assert recon_status(tmp_path, "supplier_payments", "PO-3") == "matched"
    assert recon_status(tmp_path, "supplier_payments", "PO-4") == "exception"


def test_credit_memo_must_reference_an_existing_invoice(tmp_path):
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES
        + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n"
        + "CM-1,CM,INV-1,03/04/2025,C1,1,SUB-PE,-4,250,-1000,USD\n"
        + "CM-2,CM,INV-404,03/04/2025,C1,1,SUB-PE,-4,250,-1000,USD\n",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon_status(tmp_path, "credit_memo_references", "INV-1") == "matched"
    assert recon_status(tmp_path, "credit_memo_references", "INV-404") == "exception"


def test_highest_revision_beats_newest_extract(tmp_path):
    land(
        tmp_path,
        "mes_production",
        "2025-03-04",
        "mes_production_20250304.csv",
        MES_V1 + "P1,2,2025-03-04 15:00:00,CHEM-A,B,L1,SUB-PE,7,op1,\n",
    )
    land(
        tmp_path,
        "mes_production",
        "2025-03-05",
        "mes_production_20250305.csv",
        MES_V1 + "P1,1,2025-03-04 15:00:00,CHEM-A,B,L1,SUB-PE,5,op1,\n",
    )  # stale re-send
    run(tmp_path)
    assert silver(tmp_path, "production_events", "revision, qty") == [(2, 7)]


def test_night_shift_cutoff(tmp_path):
    land(
        tmp_path,
        "mes_production",
        "2025-03-04",
        "mes_production_20250304.csv",
        MES_V1
        + "P1,1,2025-03-05 05:59:59,CHEM-A,C,L1,SUB-PE,1,op1,\n"
        + "P2,1,2025-03-05 06:00:00,CHEM-A,C,L1,SUB-PE,1,op1,\n",
    )
    run(tmp_path)
    assert silver(tmp_path, "production_events", "event_id, business_date") == [
        ("P1", date(2025, 3, 4)),
        ("P2", date(2025, 3, 5)),
    ]


def test_mass_balance_explanations_match_on_station(tmp_path):
    land(
        tmp_path,
        "mes_material_issues",
        "2025-03-04",
        "mes_material_issues_20250304.csv",
        ISSUES + "I1,2025-03-04 14:00:00,CHEM-A,B,PR-POLY,30,EA\n",
    )
    land(
        tmp_path,
        "mes_production",
        "2025-03-04",
        "mes_production_20250304.csv",
        MES_V1
        + "P1,1,2025-03-04 15:00:00,CHEM-A,B,L1,SUB-PE,10,op1,\n"
        + "P2,1,2025-03-04 15:00:00,LITHO-A,B,L1,MPU-XL-A,abc,op1,\n",
    )  # other station
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon_status(tmp_path, "mass_balance", "CHEM-A") == "exception"


def test_file_versions_follow_arrival_order(tmp_path):
    line = "INV-1,INV,,03/04/2025,C1,1,SUB-PE,{q},250,{a},USD\n"
    a, b = INVOICES + line.format(q=4, a=1000), INVOICES + line.format(q=40, a=10000)
    name = "erp_invoices_20250304.csv"

    def qty(root: Path) -> list[tuple]:
        return rows(root, "SELECT qty FROM read_parquet('@/silver/invoice_lines/*/*.parquet')")

    # Reverted in place: A, then B, then A again.
    for i, body in enumerate((a, b, a)):
        land(tmp_path / "inplace", "erp_invoices", "2025-03-04", name, body)
        path = tmp_path / "inplace/landing/erp_invoices/2025/03/04" / name
        os.utime(path, ns=(10**9 * (i + 1), 10**9 * (i + 1)))
        run(tmp_path / "inplace")
    assert qty(tmp_path / "inplace") == [(4,)]
    # Re-sent in later folders: A, B, A. Incremental and full agree.
    for arrival, body in (("2025-03-04", a), ("2025-03-05", b), ("2025-03-06", a)):
        land(tmp_path / "resent", "erp_invoices", arrival, name, body)
        run(tmp_path / "resent", arrival)
    run(tmp_path / "resent", lake="full")
    assert (
        qty(tmp_path / "resent")
        == rows(
            tmp_path / "resent",
            "SELECT qty FROM read_parquet('@/silver/invoice_lines/*/*.parquet')",
            lake="full",
        )
        == [(4,)]
    )
    # A version that lands in an earlier arrival folder than one already loaded loses.
    land(tmp_path / "early", "erp_invoices", "2025-03-06", name, a)
    run(tmp_path / "early")
    land(tmp_path / "early", "erp_invoices", "2025-03-05", name, b)
    run(tmp_path / "early")
    assert qty(tmp_path / "early") == [(4,)]
    # A header-only re-issue withdraws every record.
    land(tmp_path / "empty", "erp_invoices", "2025-03-04", name, a)
    run(tmp_path / "empty")
    land(tmp_path / "empty", "erp_invoices", "2025-03-05", name, INVOICES)
    run(tmp_path / "empty")
    assert not list((tmp_path / "empty/lake/silver/invoice_lines").rglob("*.parquet"))


def test_noop_run_reads_no_landing_files(tmp_path, monkeypatch):
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n",
    )
    run(tmp_path)
    reads = []
    original = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda self: reads.append(self) or original(self))
    run(tmp_path)
    assert [p for p in reads if "landing" in p.parts] == []


def test_concurrent_runs_are_refused(tmp_path):
    land(tmp_path, *LATE_SNAPSHOT)
    (tmp_path / "lake/_control").mkdir(parents=True)
    (tmp_path / "lake/_control/run.lock").write_text("123\n")
    with pytest.raises(RuntimeError, match="another run"):
        run(tmp_path)


def test_local_times_that_cannot_be_placed_are_quarantined(tmp_path):
    land(
        tmp_path,
        "mes_production",
        "2025-11-02",
        "mes_production_20251101.csv",
        MES_V1
        + "P1,1,2025-03-09 02:30:00,CHEM-A,C,L1,SUB-PE,1,op1,\n"  # spring-forward gap
        + "P2,1,2025-11-02 01:30:00,CHEM-A,C,L1,SUB-PE,1,op1,\n"  # fall-back hour, twice
        + "P3,1,2025-3-04 13:00:00,CHEM-A,B,L1,SUB-PE,1,op1,\n"
        + "P4,1, 2025-03-04 13:00:00,CHEM-A,B,L1,SUB-PE,1,op1,\n",
    )
    land(
        tmp_path,
        "mes_production",
        "2025-11-02",
        "mes_production_20251102.csv",
        MES_V2 + "P5,1,2025-11-02T18:00:00z,CHEM-A,2,L1,SUB-PE,1,EA,op1,\n",
    )
    run(tmp_path)
    assert rows(
        tmp_path,
        "SELECT reject_reason FROM read_parquet('@/quarantine/*/*/*.parquet') "
        "ORDER BY _file, _record_number",
    ) == [
        ("nonexistent_local_time",),
        ("ambiguous_local_time",),
        ("invalid_timestamp",),
        ("invalid_timestamp",),
        ("invalid_timestamp",),
    ]


def test_values_are_not_guessed(tmp_path):
    land(
        tmp_path,
        "bank_statements",
        "2025-03-04",
        "bank_statement_20250304.csv",
        BANK + '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
        '03/04/2025,ACH DEBIT X PO-1,"3.000,00",,"97,000.00"\r\n'
        '03/04/2025,ACH DEBIT X PO-2,"USD 1000",,"96,000.00"\r\n'
        '03/04/2025,MYSTERY,,,"96,000.00"\r\n'
        '03/04/2025,LOCKBOX DEPOSIT C1,,"-500.00","95,500.00"\r\n'
        '03/04/2025,BOTH,"1.00","1.00","95,500.00"\r\n',
        "cp1252",
    )
    land(
        tmp_path,
        "cmms_work_orders",
        "2025-03-04",
        "cmms_work_orders_20250304.jsonl",
        '{"work_order":"WO-1","event":"COMPLETED","timestamp":"2025-03-04 23:30:00",'
        '"cost":{"amount":"5000.00","currency":"USD"}}\n'
        '{"work_order":"WO-2","event":"COMPLETED","timestamp":"2025-03-04T12:00:00-06:00",'
        '"cost":{"amount":"5000.00"}}\n'
        '{"work_order":"WO-3","event":"COMPLETED","timestamp":"2025-03-04T12:00:00-06:00",'
        '"cost":{"amount":"-5000.00","currency":"USD"}}\n'
        '{"work_order":"WO-4","event":"COMPLETED","timestamp":"2025-03-04T12:00:00-06:00",'
        '"note":"a b","cost":{"amount":"10.00","currency":"USD"}}\n',
    )
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES
        + "INV-9,INV,,03/04/2025,C1,1,SUB-PE,4,-250,-1000,USD\n"
        + "INV-8,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,\n",
    )
    land(
        tmp_path,
        "erp_goods_receipts",
        "2025-03-04",
        "erp_goods_receipts_20250304.csv",
        RECEIPTS + "GR-9,PO-9,2025-03-04,PR-POLY,10,EA,-100.00,-1000.00\n",
    )
    run(tmp_path)
    assert silver(tmp_path, "bank_transactions", "description") == [("BALANCE FORWARD",)]
    assert silver(tmp_path, "work_order_events", "work_order") == [("WO-4",)]
    reasons = rows(
        tmp_path,
        "SELECT _source, reject_reason, count(*) FROM "
        "read_parquet('@/quarantine/*/*/*.parquet') GROUP BY ALL ORDER BY ALL",
    )
    assert reasons == [
        ("bank_statements", "invalid_amount", 5),
        ("cmms_work_orders", "invalid_amount", 2),
        ("cmms_work_orders", "invalid_timestamp", 1),
        ("erp_goods_receipts", "invalid_amount", 1),
        ("erp_invoices", "invalid_amount", 1),
        ("erp_invoices", "unsupported_currency", 1),
    ]


def test_unsized_quarantine_must_point_the_right_way(tmp_path):
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n",
    )
    # Deposits exceed invoices by 4,000. A deposit line with an unreadable amount can only
    # have lowered deposits, so it cannot account for that.
    land(
        tmp_path,
        "bank_statements",
        "2025-03-04",
        "bank_statement_20250304.csv",
        BANK + '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
        '03/04/2025,LOCKBOX DEPOSIT A,,"5,000.00","105,000.00"\r\n'
        '03/04/2025,LOCKBOX DEPOSIT B,,"abc","105,000.00"\r\n',
        "cp1252",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon_status(tmp_path, "customer_receipts", "ALL") == "exception"
