import ast
import hashlib
from datetime import date
from pathlib import Path

import duckdb

from factorysim.config import SimConfig
from factorysim.db import create_database
from factorysim.etl.pipeline import ENTITIES, run_pipeline
from factorysim.simulation import run_simulation
from factorysim.sources.export import export_sources

GOLD_TABLES = (
    "fct_production_daily",
    "fct_material_issues_daily",
    "fct_sales_daily",
    "fct_goods_receipts",
    "fct_maintenance",
    "fct_bank_transactions",
    "fct_stock_snapshots",
    "recon_results",
    "recon_summary",
)


def _export(root: Path, days: int = 365) -> Path:
    conn = create_database(root / "truth" / "factory.db")
    run_simulation(conn, SimConfig(days=days))
    export_sources(conn, root / "landing", root / "truth" / "defects.jsonl")
    conn.close()
    return root


def _lake_tables(lake: Path) -> dict[str, list[tuple]]:
    conn = duckdb.connect()
    tables = {}
    for e in ENTITIES:
        tables[f"silver.{e.name}"] = conn.execute(
            f"SELECT * FROM read_parquet('{lake}/silver/{e.name}/*/*.parquet') ORDER BY ALL"
        ).fetchall()
    for t in GOLD_TABLES:
        tables[f"gold.{t}"] = conn.execute(
            f"SELECT * FROM read_parquet('{lake}/gold/{t}/*.parquet') ORDER BY ALL"
        ).fetchall()
    tables["quarantine"] = conn.execute(
        f"SELECT * FROM read_parquet('{lake}/quarantine/*/*/*.parquet') ORDER BY ALL"
    ).fetchall()
    return tables


def test_every_injected_defect_is_handled(full_year):
    _, _, results = full_year
    for d in results["defects"]:
        assert d["injected"] > 0, f"no {d['defect']} defects injected; scorecard would be vacuous"
        assert d["handled"] == d["injected"], d


def test_quarantine_is_exact(full_year):
    q = full_year[2]["quarantine"]
    assert q["precision"] == 1 and q["recall"] == 1
    assert q["correct_reason"] == q["true_positives"]


def test_gold_matches_truth_except_for_injected_losses(full_year):
    for name, comparison in full_year[2]["gold_vs_truth"].items():
        assert comparison["unexplained"] == 0, (name, comparison["unexplained_examples"])


def test_reconciliation_flags_only_business_errors(full_year):
    r = full_year[2]["reconciliation"]
    assert r["unexpected_exceptions"] == []
    assert r["business_errors_flagged"] == r["expected_business_errors"] > 0


def test_rerun_without_new_files_changes_nothing(full_year, tmp_path):
    root, _, _ = full_year
    before = _lake_tables(root / "lake")
    summary = run_pipeline(root / "landing", root / "lake")
    assert summary.files_loaded == 0 and summary.duplicate_files == 0
    assert all(e.partitions_written == 0 for e in summary.entities)
    assert _lake_tables(root / "lake") == before


def test_incremental_runs_equal_one_full_run(tmp_path):
    root = _export(tmp_path, days=200)
    run_pipeline(root / "landing", root / "full")
    # Uneven batches, including one that straddles the MES schema change on 2025-07-01.
    for through in (
        "2025-01-09",
        "2025-02-15",
        "2025-02-16",
        "2025-04-30",
        "2025-06-29",
        "2025-07-03",
        "2025-08-31",
    ):
        run_pipeline(root / "landing", root / "incremental", date.fromisoformat(through))
    full, incremental = _lake_tables(root / "full"), _lake_tables(root / "incremental")
    assert full.keys() == incremental.keys()
    for table in full:
        assert incremental[table] == full[table], table


def test_export_is_deterministic(tmp_path):
    def digest(root: Path) -> str:
        h = hashlib.sha256()
        for path in sorted((root / "landing").rglob("*")):
            if path.is_file():
                h.update(path.relative_to(root).as_posix().encode())
                h.update(path.read_bytes())
        h.update((root / "truth" / "defects.jsonl").read_bytes())
        return h.hexdigest()

    assert digest(_export(tmp_path / "a", 30)) == digest(_export(tmp_path / "b", 30))


def test_pipeline_cannot_see_the_simulator():
    """The pipeline must only ever see files: no imports from the rest of factorysim."""
    etl_dir = Path(__file__).resolve().parent.parent / "factorysim" / "etl"
    for path in etl_dir.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                assert node.level <= 1, f"{path.name} imports from outside factorysim.etl"
                assert not module.startswith("factorysim") or module.startswith("factorysim.etl")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("factorysim"), path.name


# ---------------------------------------------------------------------- contract cases


def _land(landing: Path, source: str, arrival: str, name: str, content: bytes) -> None:
    path = landing / source / arrival.replace("-", "/") / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _silver(lake: Path, entity: str, columns: str) -> list[tuple]:
    return (
        duckdb.connect()
        .execute(
            f"SELECT {columns} FROM read_parquet('{lake}/silver/{entity}/*/*.parquet') ORDER BY ALL"
        )
        .fetchall()
    )


def test_mes_contract_v1_and_v2(tmp_path):
    landing = tmp_path / "landing"
    v1 = (
        "event_id,rev,event_time,station,shift,lot,sku,qty,operator,comment\n"
        # Night shift on the spring-forward night: 03:30 CDT belongs to the 8th.
        "P1,1,2025-03-09 03:30:00,lith-a ,C,L1,MPUXL-A,10,op1,\n"
        "event_id,rev,event_time,station,shift,lot,sku,qty,operator,comment\n"
        '"P2",1,2025-03-08 13:00:00,LITHO-B,B,L2,MPU-XL-B,5,op1,"multi\nline, quoted"\n'
        "P3,1,2025-03-08 13:00:00,LITHO-B,B\n"
        "P4,1,2025-03-08 13:00:00,LITHO-B,B,L2,MPU-XL-B,1O5,op1,\n"
    )
    _land(landing, "mes_production", "2025-03-09", "mes_production_20250308.csv", v1.encode())
    v2 = (
        "event_id,revision,event_ts_utc,station_code,shift_code,lot_id,item_code,quantity,"
        "uom,operator_id,comment\n"
        "P1,2,2025-03-09T08:30:00Z,LITHO-A,3,L1,MPU-XL-A,12,EA,op1,\n"
    )
    _land(landing, "mes_production", "2025-03-10", "mes_production_20250309.csv", v2.encode())
    run_pipeline(landing, tmp_path / "lake")

    rows = _silver(
        tmp_path / "lake",
        "production_events",
        "event_id, revision, business_date, event_ts_utc, station_code, sku, qty",
    )
    assert [r[:3] for r in rows] == [("P1", 2, date(2025, 3, 8)), ("P2", 1, date(2025, 3, 8))]
    assert rows[0][3].isoformat() == "2025-03-09T08:30:00"
    assert rows[0][4:] == ("LITHO-A", "MPU-XL-A", 12)
    reasons = (
        duckdb.connect()
        .execute(
            f"SELECT _record_number, reject_reason FROM "
            f"read_parquet('{tmp_path}/lake/quarantine/*/*/*.parquet') ORDER BY 1"
        )
        .fetchall()
    )
    assert reasons == [(5, "malformed_record"), (6, "invalid_quantity")]


def test_erp_and_bank_contracts(tmp_path):
    landing = tmp_path / "landing"
    invoices = (
        "document_no,doc_type,reference_doc,document_date,customer_id,line_no,sku,quantity,"
        "unit_price,line_amount,currency\r\n"
        'INV-1,INV,,03/04/2025,C1,1,mpu-xl-a,2,"$1,000.00","2,000.00",USD\r\n'
        "INV-1,INV,,03/04/2025,C1,2,SUB-PE,4,250,1000,USD\r\n"
        "CM-1,CM,INV-1,2025-03-04,C1,1,SUB-PE,-4,250.00,-1000.00,USD\r\n"
        "INV-2,INV,,03/04/2025,C1,1,SUB-PE,1,250,999,USD\r\n"
    )
    _land(
        landing,
        "erp_invoices",
        "2025-03-04",
        "erp_invoices_20250304.csv",
        invoices.encode("utf-8-sig"),
    )
    receipts = (
        "gr_number,po_number,receipt_date,material,quantity,uom,unit_cost,extended_cost\r\n"
        'GR-1,PO-1,2025-03-04,SI-WFR-300,60,BX25,1250.00,"$75,000.00"\r\n'
    )
    _land(
        landing,
        "erp_goods_receipts",
        "2025-03-04",
        "erp_goods_receipts_20250304.csv",
        receipts.encode("utf-8-sig"),
    )
    bank = (
        "Date,Description,Debit,Credit,Balance\r\n"
        '03/04/2025,BALANCE FORWARD,,,"100,000.00"\r\n'
        '03/04/2025,ACH DEBIT MÜLLER PHOTOCHEMIE GMBH PO-1,"75,000.00",,"25,000.00"\r\n'
    )
    _land(
        landing,
        "bank_statements",
        "2025-03-04",
        "bank_statement_20250304.csv",
        bank.encode("cp1252"),
    )
    run_pipeline(landing, tmp_path / "lake")
    lake = tmp_path / "lake"

    assert _silver(lake, "invoice_lines", "doc_no, line_no, sku, qty, line_amount") == [
        ("CM-1", 1, "SUB-PE", -4, -1000),
        ("INV-1", 1, "MPU-XL-A", 2, 2000),
        ("INV-1", 2, "SUB-PE", 4, 1000),
    ]
    assert _silver(lake, "goods_receipts", "qty_base, amount") == [(1500, 75000)]
    assert _silver(lake, "bank_transactions", "txn_kind, reference, amount, description") == [
        ("opening_balance", None, 0, "BALANCE FORWARD"),
        ("supplier_payment", "PO-1", -75000, "ACH DEBIT MÜLLER PHOTOCHEMIE GMBH PO-1"),
    ]
    quarantined = (
        duckdb.connect()
        .execute(f"SELECT reject_reason FROM read_parquet('{lake}/quarantine/*/*/*.parquet')")
        .fetchall()
    )
    assert quarantined == [("amount_mismatch",)]


def test_redelivered_file_is_skipped_by_content(tmp_path):
    landing = tmp_path / "landing"
    body = b"snapshot_date,material,on_hand,uom\r\n2025-03-02,PR-POLY,10,EA\r\n"
    _land(landing, "erp_stock_snapshots", "2025-03-02", "erp_stock_20250302.csv", body)
    _land(landing, "erp_stock_snapshots", "2025-03-05", "erp_stock_20250302.csv", body)
    summary = run_pipeline(landing, tmp_path / "lake")
    assert (summary.files_loaded, summary.duplicate_files) == (1, 1)
    assert _silver(tmp_path / "lake", "stock_snapshots", "on_hand") == [(10,)]
