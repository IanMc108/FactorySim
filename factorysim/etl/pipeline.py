"""Orchestrate landing -> bronze -> silver/quarantine -> gold -> reconciliation.

Run model: each run ingests whatever has landed since the last run (optionally only files
that arrived on or before ``through``), then

* recomputes each silver entity from all of its bronze and rewrites only the monthly
  partitions that the run's new files touched (dynamic partition overwrite);
* rebuilds the gold facts and reconciliation tables from silver (they are small).

Running day by day therefore produces the same silver, gold and reconciliation as one
run over everything (``tests/test_etl.py`` checks this).

This package must not import the simulator or the source exporter: it sees only files.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from importlib.resources import files
from pathlib import Path

import duckdb

from .ingest import BRONZE_COLUMNS, SOURCES, IngestResult, discover, ingest
from .lake import Lake, _q

LATE_ARRIVAL_WINDOW_DAYS = 7


@dataclass(frozen=True)
class Entity:
    name: str
    source: str
    keys: tuple[str, ...]
    # Among duplicates of one key, the first row in this order wins.
    latest_first: str = "_arrival_date DESC, _file DESC, _record_number DESC"


ENTITIES = (
    Entity(
        "production_events",
        "mes_production",
        ("event_id",),
        "revision DESC, _arrival_date DESC, _file DESC, _record_number DESC",
    ),
    Entity("material_issues", "mes_material_issues", ("issue_id",)),
    Entity("invoice_lines", "erp_invoices", ("doc_no", "line_no")),
    Entity("goods_receipts", "erp_goods_receipts", ("gr_no",)),
    Entity("stock_snapshots", "erp_stock_snapshots", ("snapshot_date", "material_code")),
    Entity("work_order_events", "cmms_work_orders", ("work_order", "event", "event_ts_utc")),
    Entity(
        "bank_transactions", "bank_statements", ("txn_date", "description", "amount", "balance")
    ),
)
QUARANTINE_SELECT = """
    SELECT _source, _file, _record_number, _arrival_date, _file_business_date,
           reject_reason, _raw, CAST(record AS VARCHAR) AS record,
           strftime(_file_business_date, '%Y-%m') AS business_month
    FROM {table} WHERE reject_reason IS NOT NULL"""


@dataclass(frozen=True)
class EntityStats:
    entity: str
    records: int
    rejected: int
    duplicates_removed: int
    rows: int
    partitions_written: int


@dataclass(frozen=True)
class RunSummary:
    batch_id: str
    files_loaded: int
    duplicate_files: int
    watermark: date | None
    entities: list[EntityStats]
    recon: dict[str, dict[str, int]]


def _sql(*parts: str) -> str:
    return files("factorysim.etl").joinpath("sql", *parts).read_text(encoding="utf-8")


def _sections(text: str, marker: str) -> list[tuple[str, str]]:
    """Split a SQL file on ``-- <marker>: <header>`` lines."""
    pieces = re.split(rf"^-- {marker}: (.+)$", text, flags=re.M)
    return [
        (pieces[i].strip(), pieces[i + 1].strip().rstrip(";")) for i in range(1, len(pieces), 2)
    ]


def _load_reference(conn: duckdb.DuckDBPyConnection) -> None:
    ref = files("factorysim.etl").joinpath("reference")
    for name in (
        "stations",
        "station_aliases",
        "products",
        "sku_aliases",
        "materials",
        "uom_conversions",
    ):
        path = _q(str(ref.joinpath(f"{name}.csv")))
        conn.execute(
            f"CREATE OR REPLACE TABLE ref_{name} AS "
            f"SELECT * FROM read_csv('{path}', header = true, auto_detect = true)"
        )


def _bronze_view(conn: duckdb.DuckDBPyConnection, lake: Lake, source: str) -> None:
    glob = lake.bronze_glob(source)
    if glob:
        body = (
            f"SELECT * REPLACE (record::JSON AS record) "
            f"FROM read_parquet('{glob}', hive_partitioning = false)"
        )
    else:
        conn.execute(f"CREATE OR REPLACE TEMP TABLE _empty_bronze ({BRONZE_COLUMNS})")
        body = "SELECT * REPLACE (record::JSON AS record) FROM _empty_bronze"
    conn.execute(f"CREATE OR REPLACE TEMP VIEW bronze AS {body}")


def _write_partitions(
    conn,
    lake: Lake,
    layer: str,
    name: str,
    table: str,
    month_expr: str,
    months: set[str],
    order_by: str,
) -> int:
    for month in sorted(months):
        target = lake.partition_file(layer, name, month)
        target.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(
            f"COPY (SELECT * FROM {table} WHERE {month_expr} = '{month}' ORDER BY {order_by}) "
            f"TO '{_q(target)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    return len(months)


def build_silver(conn, lake: Lake, new_files: set[str]) -> list[EntityStats]:
    stats = []
    for entity in ENTITIES:
        _bronze_view(conn, lake, entity.source)
        conn.execute(
            f"CREATE OR REPLACE TEMP TABLE validated AS {_sql('silver', entity.name + '.sql')}"
        )
        keys = ", ".join(entity.keys)
        conn.execute(
            f"""CREATE OR REPLACE TEMP TABLE silver_new AS
                SELECT * EXCLUDE (reject_reason, _raw, record),
                       strftime(business_date, '%Y-%m') AS business_month
                FROM validated WHERE reject_reason IS NULL
                QUALIFY row_number() OVER (
                    PARTITION BY {keys} ORDER BY {entity.latest_first}) = 1"""
        )
        conn.execute(
            "CREATE OR REPLACE TEMP TABLE quarantine_new AS "
            + QUARANTINE_SELECT.format(table="validated")
        )
        # Partitions touched by this run's files: the months of every record they contain.
        conn.execute("CREATE OR REPLACE TEMP TABLE _new_files (file VARCHAR)")
        if new_files:
            conn.executemany("INSERT INTO _new_files VALUES (?)", [(f,) for f in sorted(new_files)])
        touched = {
            m
            for (m,) in conn.execute(
                """SELECT DISTINCT strftime(coalesce(business_date, _file_business_date), '%Y-%m')
                   FROM validated WHERE _file IN (SELECT file FROM _new_files)"""
            ).fetchall()
        }
        silver_months = {
            m for (m,) in conn.execute("SELECT DISTINCT business_month FROM silver_new").fetchall()
        } & touched
        quarantine_months = {
            m
            for (m,) in conn.execute(
                "SELECT DISTINCT business_month FROM quarantine_new"
            ).fetchall()
        } & touched
        written = _write_partitions(
            conn, lake, "silver", entity.name, "silver_new", "business_month", silver_months, keys
        )
        _write_partitions(
            conn,
            lake,
            "quarantine",
            entity.source,
            "quarantine_new",
            "business_month",
            quarantine_months,
            "_file, _record_number",
        )
        records, rejected = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE reject_reason IS NOT NULL) FROM validated"
        ).fetchone()
        rows = conn.execute("SELECT count(*) FROM silver_new").fetchone()[0]
        stats.append(
            EntityStats(entity.name, records, rejected, records - rejected - rows, rows, written)
        )
    return stats


def _lake_view(conn, lake: Lake, layer: str, name: str, view: str, empty_sql: str) -> None:
    glob = lake.layer_glob(layer, name)
    if glob:
        conn.execute(
            f"CREATE OR REPLACE TEMP VIEW {view} AS "
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning = false)"
        )
    else:
        conn.execute(f"CREATE OR REPLACE TEMP VIEW {view} AS {empty_sql}")


def build_gold(conn, lake: Lake, watermark: date) -> dict[str, dict[str, int]]:
    # Read silver and quarantine back from the lake, so gold reflects persisted state only.
    for entity in ENTITIES:
        _bronze_view(conn, lake, entity.source)
        empty = (
            f"SELECT * EXCLUDE (reject_reason, _raw, record), '' AS business_month "
            f"FROM ({_sql('silver', entity.name + '.sql')}) WHERE false"
        )
        _lake_view(conn, lake, "silver", entity.name, f"silver_{entity.name}", empty)
    quarantine_parts = []
    for source in SOURCES:
        glob = lake.layer_glob("quarantine", source)
        if glob:
            quarantine_parts.append(
                f"SELECT * FROM read_parquet('{glob}', hive_partitioning = false)"
            )
    conn.execute(
        "CREATE OR REPLACE TEMP VIEW quarantine_all AS "
        + (
            " UNION ALL BY NAME ".join(quarantine_parts)
            if quarantine_parts
            else f"SELECT * FROM ({QUARANTINE_SELECT.format(table='validated')}) WHERE false"
        )
    )

    tables = []
    for name in ("stations", "products", "materials"):
        conn.execute(f"CREATE OR REPLACE TEMP VIEW dim_{name[:-1]} AS SELECT * FROM ref_{name}")
        tables.append(f"dim_{name[:-1]}")
    for table, sql in _sections(_sql("gold", "facts.sql"), "table"):
        conn.execute(f"CREATE OR REPLACE TEMP TABLE {table} AS {sql}")
        tables.append(table)

    checks = []
    for header, sql in _sections(_sql("gold", "recon_checks.sql"), "check"):
        name, sources = (part.strip() for part in header.split("|"))
        related = ", ".join(f"'{s.strip()}'" for s in sources.split(","))
        checks.append(
            f"""SELECT '{name}' AS check_name, c.business_date, c.key,
                       CAST(c.expected AS DECIMAL(38, 2)) AS expected,
                       CAST(c.actual AS DECIMAL(38, 2)) AS actual,
                       CAST(c.actual - c.expected AS DECIMAL(38, 2)) AS variance,
                       (SELECT count(*) FROM quarantine_all q
                        WHERE q._source IN ({related})
                          AND q._file_business_date
                              BETWEEN coalesce(c.window_start, c.business_date) AND c.business_date
                       ) AS related_quarantined
                FROM ({sql}) c"""
        )
    conn.execute(
        f"""CREATE OR REPLACE TEMP TABLE recon_results AS
            SELECT *,
                   CASE WHEN variance = 0 THEN 'matched'
                        WHEN related_quarantined > 0 THEN 'explained_by_quarantine'
                        WHEN business_date > DATE '{watermark}' - {LATE_ARRIVAL_WINDOW_DAYS}
                            THEN 'open_late_window'
                        ELSE 'exception'
                   END AS status
            FROM ({" UNION ALL ".join(checks)})
            ORDER BY check_name, business_date, key"""
    )
    tables.append("recon_results")
    conn.execute(
        """CREATE OR REPLACE TEMP TABLE recon_summary AS
           SELECT check_name, count(*) AS rows,
                  count(*) FILTER (WHERE status = 'matched') AS matched,
                  count(*) FILTER (WHERE status = 'explained_by_quarantine') AS explained,
                  count(*) FILTER (WHERE status = 'open_late_window') AS open,
                  count(*) FILTER (WHERE status = 'exception') AS exceptions
           FROM recon_results GROUP BY check_name ORDER BY check_name"""
    )
    tables.append("recon_summary")
    conn.execute(
        """CREATE OR REPLACE TEMP TABLE dq_quarantine_summary AS
           SELECT _source AS source, reject_reason, count(*) AS records
           FROM quarantine_all GROUP BY ALL ORDER BY ALL"""
    )
    tables.append("dq_quarantine_summary")

    for table in tables:
        target = lake.gold_file(table)
        target.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(f"COPY (SELECT * FROM {table}) TO '{_q(target)}' (FORMAT PARQUET)")
    return {
        name: {"rows": rows, "matched": m, "explained": e, "open": o, "exceptions": x}
        for name, rows, m, e, o, x in conn.execute("SELECT * FROM recon_summary").fetchall()
    }


def run_pipeline(landing: Path, lake_root: Path, through: date | None = None) -> RunSummary:
    lake = Lake(lake_root)
    lake_root.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    try:
        conn.execute("SET TimeZone = 'UTC'")
        _load_reference(conn)
        batch_id = lake.next_batch_id()
        results: list[IngestResult] = ingest(conn, lake, discover(landing, through), batch_id)
        loaded = {r.file.relative for r in results if r.status == "loaded"}
        entities = build_silver(conn, lake, loaded)
        watermark = lake.watermark(conn) or date.min
        recon = build_gold(conn, lake, watermark)
    finally:
        conn.close()
    lake.write_catalog()
    return RunSummary(
        batch_id=batch_id,
        files_loaded=len(loaded),
        duplicate_files=sum(r.status == "duplicate_content" for r in results),
        watermark=watermark,
        entities=entities,
        recon=recon,
    )
