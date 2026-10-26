"""Hostile inputs outside the exporter's defect catalog.

Each case reproduces a failure found in an adversarial review round (docs/review-log.md)
and pins the fixed behaviour. None of these inputs can come from the exporter, so they
check the pipeline's rules rather than its agreement with the generator.
"""

import os
import shutil
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
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


def test_evidence_matches_on_key(tmp_path):
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
    # A junk MPU-XL-A row is not evidence for a million-unit SUB-PE billing variance.
    assert ("SUB-PE", 10, 1000010, "exception") in recon(tmp_path, "production_vs_sales")
    assert evidence(tmp_path, "production_vs_sales", "SUB-PE") == ("exception", 0)


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


def test_lost_control_table_is_rebuilt_from_bronze_and_bronze_is_never_deleted(tmp_path):
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n",
    )
    run(tmp_path)
    bronze = next((tmp_path / "lake/bronze/erp_invoices").rglob("*.parquet"))
    count = "SELECT count(*) FROM read_parquet('@/silver/invoice_lines/*/*.parquet')"
    # The ingest log is lost (or a crash hit between writing bronze and logging it). The
    # bronze file carries its own ingest metadata, so the log entry is rebuilt from it and
    # the landed file is not loaded a second time.
    shutil.rmtree(tmp_path / "lake/_control/ingest_log")
    summary = run(tmp_path)
    assert bronze.exists() and summary.files_loaded == 0
    assert rows(tmp_path, count) == [(1,)]
    # A bronze file nobody can account for is set aside for a person, not deleted.
    stray = bronze.parent / "erp_invoices_20250305__000000000000.parquet"
    pq.write_table(pa.table({"x": [1]}), stray)
    content = stray.read_bytes()
    run(tmp_path)
    assert not stray.exists()
    moved = tmp_path / "lake/_orphaned" / stray.relative_to(tmp_path / "lake")
    assert moved.read_bytes() == content
    # A logged bronze file that has gone missing is rebuilt from the landing zone.
    bronze.unlink()
    run(tmp_path)
    assert bronze.exists()
    assert rows(tmp_path, count) == [(1,)]


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


def evidence(root: Path, check: str, key: str) -> tuple[str, int]:
    """(status, number of quarantined records attached as evidence) of one recon row."""
    found = rows(
        root,
        "SELECT status, candidate_records FROM read_parquet('@/gold/recon_results/*.parquet') "
        f"WHERE check_name = '{check}' AND key = '{key}'",
    )
    assert len(found) == 1, (check, key, recon(root, check))
    return found[0]


def silver(root: Path, entity: str, columns: str) -> list[tuple]:
    return rows(
        root, f"SELECT {columns} FROM read_parquet('@/silver/{entity}/*/*.parquet') ORDER BY ALL"
    )


def test_quarantine_is_evidence_and_never_clears_a_variance(tmp_path):
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
    # The lost rows may well account for the 20-unit mass-balance gap, but that is for a
    # person to decide: the row is an exception with both records attached.
    assert evidence(tmp_path, "mass_balance", "CHEM-A") == ("exception", 2)
    # The same records are attached to the billing variance, which they cannot explain;
    # attaching them changes nothing about its status.
    assert evidence(tmp_path, "production_vs_sales", "SUB-PE") == ("exception", 2)


def test_quarantine_of_unknown_key_is_evidence_for_any_key(tmp_path):
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
    )  # unknown SKU, qty 4: it could be the missing SUB-PE billing, nobody can tell
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert evidence(tmp_path, "production_vs_sales", "SUB-PE") == ("exception", 1)


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
    # copy of line 2 must not be offered as the reason.
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
    # The re-sent line accounts for the rejected one, so it is not offered as evidence.
    assert evidence(tmp_path, "production_vs_sales", "SUB-PE") == ("exception", 0)
    assert rows(
        tmp_path,
        "SELECT records, released_by_later_load FROM "
        "read_parquet('@/gold/dq_quarantine_summary/*.parquet')",
    ) == [(1, 1)]


def test_quarantined_invoice_is_evidence_for_a_deposit_gap(tmp_path):
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
    assert evidence(tmp_path, "customer_receipts", "ALL") == ("exception", 1)


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
    assert evidence(tmp_path, "production_vs_sales", "SUB-PE") == ("exception", 0)


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


def test_mass_balance_evidence_matches_on_station(tmp_path):
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
    assert evidence(tmp_path, "mass_balance", "CHEM-A") == ("exception", 0)


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


def test_deposit_gap_with_an_unreadable_deposit_line_stays_an_exception(tmp_path):
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n",
    )
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
    assert evidence(tmp_path, "customer_receipts", "ALL") == ("exception", 1)


# ---------------------------------------------------------------------------- round 3


def test_unreferenced_bank_debit_and_balance_jump_are_exceptions(tmp_path):
    # A debit nobody can tie to a PO or work order, with the bank's own balances
    # consistent around it; and a next-day BALANCE FORWARD that does not carry the
    # previous close forward.
    land(
        tmp_path,
        "bank_statements",
        "2025-03-04",
        "bank_statement_20250304.csv",
        BANK + '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
        '03/04/2025,ACH DEBIT GLOBEX TRADING LLC 88123,"25,000.00",,"75,000.00"\r\n',
        "cp1252",
    )
    land(
        tmp_path,
        "bank_statements",
        "2025-03-05",
        "bank_statement_20250305.csv",
        BANK + '03/05/2025,BALANCE FORWARD,,,"1,075,000.00"\r\n',
        "cp1252",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon(tmp_path, "unreferenced_bank_lines") == [
        (
            "bank_statement_20250304.csv#3: ACH DEBIT GLOBEX TRADING LLC 88123",
            0,
            -25000,
            "exception",
        )
    ]
    continuity = {k: s for k, _, _, s in recon(tmp_path, "bank_balance_continuity")}
    assert continuity == {
        "bank_statement_20250304.csv#3: ACH DEBIT GLOBEX TRADING LLC 88123": "matched",
        "bank_statement_20250305.csv#2: BALANCE FORWARD": "exception",
    }


@pytest.mark.parametrize("correction_lands_first", [False, True])
def test_same_day_extracts_are_ordered_by_arrival_time(tmp_path, correction_lands_first):
    """Two extracts of the same day land in one folder, on either side of a run.

    The one that arrived later (by its timestamp) wins, whatever its name and whichever
    the incremental run happened to see first, so incremental and full runs agree.
    """
    line = "INV-1,INV,,03/10/2025,C1,1,SUB-PE,{q},250,{a},USD\n"
    files = [
        # A 15:00 re-extract, under a name that sorts before the original.
        ("erp_invoices_1500_20250310.csv", line.format(q=5, a=1250), 1741618800),
        # The 09:46 original; when it lands second, it was delivered late, mtime preserved.
        ("erp_invoices_20250310.csv", line.format(q=4, a=1000), 1741600000),
    ]
    if not correction_lands_first:
        files.reverse()
    for name, body, ts in files:
        path = land(tmp_path, "erp_invoices", "2025-03-10", name, INVOICES + body)
        os.utime(path, (ts, ts))
        run(tmp_path, lake="incremental")
    run(tmp_path, lake="full")
    sql = "SELECT qty FROM read_parquet('@/silver/invoice_lines/*/*.parquet')"
    assert rows(tmp_path, sql, "incremental") == rows(tmp_path, sql, "full") == [(5,)]


def test_empty_redelivery_is_rejected_and_a_shrinking_reissue_is_flagged(tmp_path):
    snapshot = "snapshot_date,material,on_hand,uom\n"
    land(
        tmp_path,
        "erp_stock_snapshots",
        "2025-03-09",
        "erp_stock_20250309.csv",
        snapshot + "2025-03-09,PR-POLY,10,EA\n2025-03-09,SI-WFR-300,20,EA\n",
    )
    run(tmp_path)
    # A failed re-upload leaves a 0-byte copy in a later folder: rejected, data kept.
    land(tmp_path, "erp_stock_snapshots", "2025-03-21", "erp_stock_20250309.csv", "")
    summary = run(tmp_path)
    assert summary.rejected_files[0][0].endswith("2025/03/21/erp_stock_20250309.csv")
    count = "SELECT count(*) FROM read_parquet('@/silver/stock_snapshots/*/*.parquet')"
    assert rows(tmp_path, count) == [(2,)]
    assert recon(tmp_path, "file_version_shrink") == []
    # A readable re-issue with fewer records replaces the old one, and is raised.
    land(
        tmp_path,
        "erp_stock_snapshots",
        "2025-03-22",
        "erp_stock_20250309.csv",
        snapshot + "2025-03-09,PR-POLY,10,EA\n",
    )
    run(tmp_path)
    assert rows(tmp_path, count) == [(1,)]
    assert recon(tmp_path, "file_version_shrink") == [("erp_stock_20250309.csv", 2, 1, "exception")]


def test_implausible_files_are_rejected_and_do_not_move_the_watermark(tmp_path):
    land(tmp_path, *LATE_SNAPSHOT)
    assert run(tmp_path).watermark == date(2025, 6, 30)
    # Junk in a far-future folder, and a real-looking file that "arrived" a year after
    # its business date, are rejected; neither moves the late-arrival window.
    land(tmp_path, "cmms_work_orders", "2026-12-31", "notes.txt", "hello")
    land(
        tmp_path,
        "erp_stock_snapshots",
        "2026-07-01",
        "erp_stock_20250701.csv",
        "snapshot_date,material,on_hand,uom\n2025-07-01,PR-POLY,1,EA\n",
    )
    summary = run(tmp_path)
    assert len(summary.rejected_files) == 2
    assert summary.watermark == date(2025, 6, 30)


def test_same_size_correction_with_preserved_mtime_is_loaded(tmp_path):
    line = "INV-1,INV,,03/04/2025,C1,1,SUB-PE,{q},250,{a},USD\n"
    path = land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + line.format(q=4, a="1000.00"),
    )
    run(tmp_path)
    # `cp -p`, `rsync -t` and S3 sync from a mirror keep the old mtime: size and mtime
    # match, but the change time does not.
    st = path.stat()
    path.write_text(INVOICES + line.format(q=5, a="1250.00"))
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns))
    assert path.stat().st_size == st.st_size
    assert run(tmp_path).files_loaded == 1
    assert rows(tmp_path, "SELECT qty FROM read_parquet('@/silver/invoice_lines/*/*.parquet')") == [
        (5,)
    ]


def test_quarantined_newer_revision_is_not_released_by_an_older_one(tmp_path):
    land(
        tmp_path,
        "mes_material_issues",
        "2025-03-10",
        "mes_material_issues_20250310.csv",
        ISSUES + "I1,2025-03-10 06:05:00,LITHO-A,A,SI-WFR-300,40,EA\n",
    )
    land(
        tmp_path,
        "mes_production",
        "2025-03-10",
        "mes_production_20250310.csv",
        MES_V1 + "P1,1,2025-03-10 09:00:00,LITHO-A,A,L1,MPU-XL-SCRAP,36,op1,\n",
    )
    # Revision 2 corrects the quantity but carries an unknown station, so it is quarantined.
    land(
        tmp_path,
        "mes_production",
        "2025-03-11",
        "mes_production_20250311.csv",
        MES_V1 + "P1,2,2025-03-10 09:00:00,LITHO-AA,A,L1,MPU-XL-SCRAP,40,op1,\n",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert silver(tmp_path, "production_events", "revision, qty") == [(1, 36)]
    # The stale revision 1 in silver does not account for it: it stays evidence.
    assert evidence(tmp_path, "mass_balance", "LITHO-A") == ("exception", 1)
    assert rows(
        tmp_path,
        "SELECT sum(released_by_later_load) FROM "
        "read_parquet('@/gold/dq_quarantine_summary/*.parquet')",
    ) == [(0,)]


def test_missing_shift_and_bad_digit_grouping_are_quarantined(tmp_path):
    land(
        tmp_path,
        "mes_material_issues",
        "2025-03-11",
        "mes_material_issues_20250311.csv",
        "issue_id,issue_time_local,station,material,qty,uom\n"
        "I1,2025-03-11 06:22:01,LITHO-A,SI-WFR-300,47,EA\n",
    )  # no shift column at all
    land(
        tmp_path,
        "erp_goods_receipts",
        "2025-03-11",
        "erp_goods_receipts_20250311.csv",
        RECEIPTS + 'GR-1,PO-1,2025-03-11,PR-POLY,10,EA,100,"1,0000.00"\n',
    )
    run(tmp_path)
    assert rows(
        tmp_path,
        "SELECT reject_reason FROM read_parquet('@/quarantine/*/*/*.parquet') ORDER BY ALL",
    ) == [("invalid_amount",), ("invalid_shift",)]


def test_cli_exit_codes_signal_rejected_files_and_new_exceptions(tmp_path, capsys):
    from factorysim.cli import main

    land(tmp_path, *LATE_SNAPSHOT)
    land(tmp_path, "cmms_work_orders", "2025-06-30", "notes.txt", "hello")
    args = ["etl", "--landing", str(tmp_path / "landing"), "--lake", str(tmp_path / "lake")]
    for _ in range(2):  # a scheduler retry fails again while the file is still there
        with pytest.raises(SystemExit) as exit_info:
            main([*args, "--fail-on-new-exceptions"])
        assert exit_info.value.code == 3
        assert "REJECTED" in capsys.readouterr().out
    (tmp_path / "landing/cmms_work_orders/2025/06/30/notes.txt").unlink()
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n",
    )  # billed, never produced: a new exception
    with pytest.raises(SystemExit) as exit_info:
        main([*args, "--fail-on-new-exceptions"])
    assert exit_info.value.code == 2
    main([*args, "--fail-on-new-exceptions"])  # nothing new: exits normally
    with pytest.raises(SystemExit) as exit_info:  # but the exception is still outstanding
        main([*args, "--fail-on-exceptions"])
    assert exit_info.value.code == 2


def test_overwrite_with_an_older_timestamp_is_the_current_version(tmp_path):
    """A file overwritten in place by an older copy (restored with `cp -p`) is what is there."""
    line = "INV-1,INV,,03/10/2025,C1,1,SUB-PE,{q},250,{a},USD\n"
    for q, a, ts in ((5, 1250, 1741618800), (4, 1000, 1741600000)):
        path = land(
            tmp_path,
            "erp_invoices",
            "2025-03-10",
            "erp_invoices_20250310.csv",
            INVOICES + line.format(q=q, a=a),
        )
        os.utime(path, (ts, ts))
        run(tmp_path, lake="incremental")
    run(tmp_path, lake="full")
    sql = "SELECT qty FROM read_parquet('@/silver/invoice_lines/*/*.parquet')"
    assert rows(tmp_path, sql, "incremental") == rows(tmp_path, sql, "full") == [(4,)]


def test_release_needs_a_load_at_least_as_new_on_every_count(tmp_path):
    # Revision 1 and 2 of one event in the same file; revision 2 has an unknown station.
    land(
        tmp_path,
        "mes_production",
        "2025-03-10",
        "mes_production_20250310.csv",
        MES_V1
        + "P1,1,2025-03-10 09:00:00,LITHO-A,A,L1,MPU-XL-SCRAP,36,op1,\n"
        + "P1,2,2025-03-10 09:00:00,LITHO-AA,A,L1,MPU-XL-SCRAP,40,op1,\n",
    )
    # Invoice line 2 is fine in the first extract and broken in the re-extract (stale data
    # stays loaded); line 3 is broken in the first and fixed in the re-extract (released).
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES
        + "INV-1,INV,,03/04/2025,C1,2,SUB-PE,4,250,1000,USD\n"
        + "INV-1,INV,,03/04/2025,C1,3,SUB-PE,4,250,9999,USD\n",
    )
    land(
        tmp_path,
        "erp_invoices",
        "2025-03-05",
        "erp_invoices_20250305.csv",
        INVOICES
        + "INV-1,INV,,03/04/2025,C1,2,SUB-PE,4,250,9999,USD\n"
        + "INV-1,INV,,03/04/2025,C1,3,SUB-PE,4,250,1000,USD\n",
    )
    run(tmp_path)
    assert rows(
        tmp_path,
        "SELECT source, sum(records)::INT, sum(released_by_later_load)::INT FROM "
        "read_parquet('@/gold/dq_quarantine_summary/*.parquet') GROUP BY ALL ORDER BY ALL",
    ) == [("erp_invoices", 2, 1), ("mes_production", 1, 0)]


# ---------------------------------------------------------------------------- round 4

SNAPSHOT = "snapshot_date,material,on_hand,uom\n"


def test_balance_forward_line_that_moves_money_is_unreferenced(tmp_path):
    land(
        tmp_path,
        "bank_statements",
        "2025-03-04",
        "bank_statement_20250304.csv",
        BANK + '03/04/2025,BALANCE FORWARD,,,"1,000,000.00"\r\n'
        '03/04/2025,BALANCE FORWARD,"250,000.00",,"750,000.00"\r\n',
        "cp1252",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon(tmp_path, "unreferenced_bank_lines") == [
        ("bank_statement_20250304.csv#3: BALANCE FORWARD", 0, -250000, "exception")
    ]


def test_unreferenced_bank_credit_is_an_exception(tmp_path):
    land(
        tmp_path,
        "bank_statements",
        "2025-03-04",
        "bank_statement_20250304.csv",
        BANK + '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
        '03/04/2025,WIRE IN ACME HOLDINGS,,"10,000.00","110,000.00"\r\n',
        "cp1252",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert recon(tmp_path, "unreferenced_bank_lines") == [
        ("bank_statement_20250304.csv#3: WIRE IN ACME HOLDINGS", 0, 10000, "exception")
    ]


def test_back_valued_bank_line_keeps_statement_order(tmp_path):
    # A 03/04 debit printed on the 03/05 statement: balances are in statement order.
    for day, body in (
        ("2025-03-04", '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'),
        (
            "2025-03-05",
            '03/05/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
            '03/04/2025,ACH DEBIT MULLER PO-1,"500.00",,"99,500.00"\r\n',
        ),
        ("2025-03-06", '03/06/2025,BALANCE FORWARD,,,"99,500.00"\r\n'),
    ):
        name = f"bank_statement_{day.replace('-', '')}.csv"
        land(tmp_path, "bank_statements", day, name, BANK + body, "cp1252")
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert {s for *_, s in recon(tmp_path, "bank_balance_continuity")} == {"matched"}


def test_misplaced_copy_is_rejected_and_a_stale_resend_is_raised(tmp_path):
    line = "INV-1,INV,,03/04/2025,C1,1,SUB-PE,{q},250,{a},USD\n"
    name = "erp_invoices_20250304.csv"
    original = INVOICES + line.format(q=4, a=1000)
    land(tmp_path, "erp_invoices", "2025-03-04", name, original)
    land(tmp_path, *LATE_SNAPSHOT)
    # A byte-identical copy in a mis-typed folder (2026 for 2025) is rejected like any
    # other misplaced file: it is neither a duplicate nor the current version, and it does
    # not move the watermark.
    land(tmp_path, "erp_invoices", "2026-03-05", name, original)
    summary = run(tmp_path)
    assert [f for f, _ in summary.rejected_files] == [f"erp_invoices/2026/03/05/{name}"]
    assert summary.watermark == date(2025, 6, 30)
    qty = "SELECT qty FROM read_parquet('@/silver/invoice_lines/*/*.parquet')"
    land(tmp_path, "erp_invoices", "2025-03-06", name, INVOICES + line.format(q=5, a=1250))
    run(tmp_path)
    assert rows(tmp_path, qty) == [(5,)]
    assert recon(tmp_path, "file_version_revert") == []
    # A stale re-send of the original after the correction becomes current (latest arrival
    # wins, as for a deliberate rollback), but it is raised for a person to confirm.
    land(tmp_path, "erp_invoices", "2025-03-07", name, original)
    run(tmp_path)
    assert rows(tmp_path, qty) == [(4,)]
    assert recon(tmp_path, "file_version_revert") == [(name, 0, 1, "exception")]


@pytest.mark.parametrize(("first", "second", "expected"), [(2, 1, "exception"), (1, 2, "matched")])
def test_shrink_check_follows_the_current_version(tmp_path, first, second, expected):
    """Overwritten in place by a copy with an older timestamp: the check compares the same
    two versions that silver uses."""
    lines = ["2025-03-09,PR-POLY,10,EA\n", "2025-03-09,SI-WFR-300,20,EA\n"]
    land(tmp_path, *LATE_SNAPSHOT)
    for n, ts in ((first, 1741600000), (second, 1741500000)):
        path = land(
            tmp_path,
            "erp_stock_snapshots",
            "2025-03-09",
            "erp_stock_20250309.csv",
            SNAPSHOT + "".join(lines[:n]),
        )
        os.utime(path, (ts, ts))
        run(tmp_path)
    count = "SELECT count(*) FROM read_parquet('@/silver/stock_snapshots/*/*.parquet')"
    assert rows(tmp_path, count) == [(second + 1,)]  # + the LATE_SNAPSHOT row
    assert recon(tmp_path, "file_version_shrink") == [
        ("erp_stock_20250309.csv", max(first, second), second, expected)
    ]


def test_touching_a_rejected_file_keeps_it_rejected(tmp_path):
    land(tmp_path, *LATE_SNAPSHOT)
    notes = land(tmp_path, "cmms_work_orders", "2026-12-31", "notes.txt", "hello")
    run(tmp_path)
    os.utime(notes, (1800000000, 1800000000))  # chmod, touch or a same-bytes re-upload
    summary = run(tmp_path)
    assert summary.watermark == date(2025, 6, 30)
    assert [f for f, _ in summary.rejected_files] == ["cmms_work_orders/2026/12/31/notes.txt"]


def test_arrival_window_boundaries(tmp_path):
    def snapshot(arrival: str, day: str) -> None:
        name = f"erp_stock_{day.replace('-', '')}.csv"
        land(tmp_path, "erp_stock_snapshots", arrival, name, SNAPSHOT + f"{day},PR-POLY,1,EA\n")

    snapshot("2025-04-30", "2025-03-01")  # 60 days late: accepted
    snapshot("2025-05-02", "2025-03-02")  # 61 days late: rejected
    snapshot("2025-03-09", "2025-03-10")  # arrived before its business date: rejected
    summary = run(tmp_path)
    assert sorted(f for f, _ in summary.rejected_files) == [
        "erp_stock_snapshots/2025/03/09/erp_stock_20250310.csv",
        "erp_stock_snapshots/2025/05/02/erp_stock_20250302.csv",
    ]
    assert summary.files_loaded == 1


def test_empty_jsonl_is_a_day_without_events(tmp_path):
    land(
        tmp_path,
        "cmms_work_orders",
        "2025-03-04",
        "cmms_work_orders_20250304.jsonl",
        '{"work_order":"WO-1","event":"OPENED","timestamp":"2025-03-04T08:00:00-06:00",'
        '"asset":"LITHO-BAY"}\n'
        '{"work_order":"WO-1","event":"COMPLETED","timestamp":"2025-03-04T12:00:00-06:00",'
        '"asset":"LITHO-BAY","cost":{"amount":"5000.00","currency":"USD"}}\n',
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    # JSON lines have no header, so an empty file is how a quiet day, or a re-issue that
    # withdraws a day, is delivered. Neither is rejected; the withdrawal is raised.
    land(tmp_path, "cmms_work_orders", "2025-03-05", "cmms_work_orders_20250305.jsonl", "")
    land(tmp_path, "cmms_work_orders", "2025-03-06", "cmms_work_orders_20250304.jsonl", "")
    summary = run(tmp_path)
    assert summary.rejected_files == [] and summary.files_loaded == 2
    assert rows(tmp_path, "SELECT * FROM read_parquet('@/gold/fct_maintenance/*.parquet')") == []
    assert recon(tmp_path, "file_version_shrink") == [
        ("cmms_work_orders_20250304.jsonl", 2, 0, "exception")
    ]


def test_payment_evidence_covers_the_payment_window(tmp_path):
    land(
        tmp_path,
        "erp_goods_receipts",
        "2025-03-04",
        "erp_goods_receipts_20250304.csv",
        RECEIPTS + "GR-1,PO-1,2025-03-04,PR-POLY,10,EA,100,1000.00\n",
    )
    # The payment two days later has an amount in the wrong format, so it is quarantined.
    land(
        tmp_path,
        "bank_statements",
        "2025-03-06",
        "bank_statement_20250306.csv",
        BANK + '03/06/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
        '03/06/2025,ACH DEBIT MULLER PO-1,"1.000,00",,"99,000.00"\r\n',
        "cp1252",
    )
    land(tmp_path, *LATE_SNAPSHOT)
    run(tmp_path)
    assert evidence(tmp_path, "supplier_payments", "PO-1") == ("exception", 1)


def test_lost_bronze_without_a_landing_file_is_reported(tmp_path, capsys):
    from factorysim.cli import main

    path = land(
        tmp_path,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        INVOICES + "INV-1,INV,,03/04/2025,C1,1,SUB-PE,4,250,1000,USD\n",
    )
    run(tmp_path)
    # Bronze lost, and the landing file already expired by an S3 lifecycle rule.
    next((tmp_path / "lake/bronze/erp_invoices").rglob("*.parquet")).unlink()
    path.unlink()
    assert run(tmp_path).lost_files == ["erp_invoices/2025/03/04/erp_invoices_20250304.csv"]
    with pytest.raises(SystemExit) as exit_info:
        main(["etl", "--landing", str(tmp_path / "landing"), "--lake", str(tmp_path / "lake")])
    assert exit_info.value.code == 3
    assert "LOST erp_invoices/2025/03/04/erp_invoices_20250304.csv" in capsys.readouterr().out


def test_crash_during_log_compaction_is_repaired(tmp_path):
    land(
        tmp_path,
        "erp_stock_snapshots",
        "2025-03-09",
        "erp_stock_20250309.csv",
        SNAPSHOT + "2025-03-09,PR-POLY,10,EA\n",
    )
    run(tmp_path)
    # A crash after batch.parquet was written but before the per-file entries it replaced
    # were removed leaves every entry of the batch on disk twice.
    batch = next((tmp_path / "lake/_control/ingest_log").glob("batch_id=*"))
    shutil.copy(batch / "batch.parquet", batch / "00000001.parquet")
    run(tmp_path)
    log = "SELECT count(*) FROM read_parquet('@/_control/ingest_log/*/*.parquet')"
    assert rows(tmp_path, log) == [(1,)]
    assert recon(tmp_path, "file_version_shrink") == []
