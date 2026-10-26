"""Hostile inputs outside the exporter's defect catalog.

Each case reproduces a failure found by adversarial review round 1 (docs/review-log.md)
and pins the fixed behaviour. None of these inputs can come from the exporter, so they
check the pipeline's rules rather than its agreement with the generator.
"""

from datetime import date
from pathlib import Path

import duckdb

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
