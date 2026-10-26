"""Orchestrate landing -> bronze -> silver/quarantine -> gold -> reconciliation.

Run model: each run ingests whatever has landed since the last run (optionally only files
that arrived on or before ``through``), then

* recomputes each silver entity from all of its bronze and rewrites only the monthly
  partitions whose content changed, deleting any left empty (a diff-based dynamic
  partition overwrite: a correction that moves a record to another month rewrites both);
* rebuilds the gold facts and reconciliation tables from silver (they are small).

Running day by day therefore produces the same silver, gold and reconciliation as one
run over everything (``tests/test_etl.py`` checks this).

This package must not import the simulator or the source exporter: it sees only files.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from datetime import date
from importlib.resources import files
from pathlib import Path

import duckdb

from .ingest import BRONZE_COLUMNS, SOURCES, IngestResult, discover, ingest
from .lake import Lake, _q, atomic_copy

LATE_ARRIVAL_WINDOW_DAYS = 7
PAYMENT_TERMS_DAYS = 3
STATUSES = ("matched", "open_late_window", "exception")


@dataclass(frozen=True)
class Entity:
    name: str
    source: str
    keys: tuple[str, ...]
    # Among duplicates of one key, the first row in this order wins: the newest extract
    # (file business date), then the file that landed last (arrival folder, then arrival
    # time), then the last record. Ingest order is only a final tie-break.
    latest_first: str = (
        "_file_business_date DESC, _arrival_date DESC, _arrival_ts DESC, _ingest_seq DESC, "
        "_record_number DESC"
    )


ENTITIES = (
    Entity(
        "production_events",
        "mes_production",
        ("event_id",),
        "revision DESC, _file_business_date DESC, _arrival_date DESC, _arrival_ts DESC, "
        "_ingest_seq DESC, _record_number DESC",
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
# Parsing helpers shared by the silver SQL. Integers must be plain digits ("10.5", "1e3"
# and "1_000" are rejected, never rounded); money may carry "$", "," and spaces.
MACROS = r"""
CREATE OR REPLACE MACRO strict_int(s) AS
    CASE WHEN regexp_full_match(trim(s), '-?[0-9]+') THEN TRY_CAST(trim(s) AS BIGINT) END;
-- Timestamps must match their pattern exactly (no single-digit fields or stray spaces).
CREATE OR REPLACE MACRO strict_ts(s, pattern, fmt) AS
    CASE WHEN regexp_full_match(s, pattern) THEN try_strptime(s, fmt) END;
CREATE OR REPLACE MACRO local_to_utc(t) AS timezone('UTC', timezone('America/Chicago', t));
CREATE OR REPLACE MACRO utc_to_local(t) AS timezone('America/Chicago', timezone('UTC', t));
-- A plant-local wall time that does not exist (spring-forward gap) or exists twice
-- (fall-back hour) cannot be placed on the UTC timeline without guessing.
CREATE OR REPLACE MACRO local_time_problem(t) AS
    CASE WHEN t IS NULL THEN NULL
         WHEN utc_to_local(local_to_utc(t)) <> t THEN 'nonexistent_local_time'
         WHEN utc_to_local(local_to_utc(t) + INTERVAL 1 HOUR) = t
              OR utc_to_local(local_to_utc(t) - INTERVAL 1 HOUR) = t
             THEN 'ambiguous_local_time'
    END;
-- Money: an optional "$", then digits with correctly placed thousands separators (or
-- none), then up to 4 decimals. "1,0,0" and "12.345,6" are rejected, not reinterpreted.
CREATE OR REPLACE MACRO money(s) AS
    CASE WHEN regexp_full_match(regexp_replace(s, '[$\s]', '', 'g'),
                                '-?([0-9]{1,3}(,[0-9]{3})+|[0-9]+)(\.[0-9]{1,4})?')
         THEN TRY_CAST(regexp_replace(s, '[$,\s]', '', 'g') AS DECIMAL(18, 4)) END;
"""
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
    # Landed files whose latest log entry is a rejection, reported on every run until the
    # file is fixed or removed (not only on the run that first saw it).
    rejected_files: list[tuple[str, str | None]]
    # Current file versions whose bronze and landing file are both gone: data lost.
    lost_files: list[str]
    watermark: date | None
    entities: list[EntityStats]
    recon: dict[str, dict[str, int]]
    changes: dict[str, int]


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
    """Bronze for one feed, keeping only the records of each file name's current version.

    The current version comes from the ingest log (``Lake.current_versions``), so a file
    re-issued under the same name replaces every record of its earlier versions, including
    when the re-issue is a revert or has no records at all.
    """
    glob = lake.bronze_glob(source)
    if glob:
        relation = f"read_parquet('{glob}', hive_partitioning = false)"
    else:
        conn.execute(f"CREATE OR REPLACE TEMP TABLE _empty_bronze ({BRONZE_COLUMNS})")
        relation = "_empty_bronze"
    conn.execute(
        f"""CREATE OR REPLACE TEMP VIEW bronze AS
            SELECT b.* REPLACE (b.record::JSON AS record) FROM {relation} b
            SEMI JOIN current_versions v
                ON v.source = b._source
               AND v.file_name = regexp_extract(b._file, '[^/]+$')
               AND v.sha256 = b._file_sha256"""
    )


def _sync_partitions(conn, lake: Lake, layer: str, name: str, table: str, order_by: str) -> int:
    """Make ``<layer>/<name>`` match ``table`` by rewriting only months whose content changed.

    Each month is fingerprinted (row count and sum of row hashes) on both sides; months
    that differ are rewritten atomically and months with no rows left are deleted. The
    partition column lives only in the path, as Glue and Athena expect.
    """
    described = conn.execute(f"DESCRIBE {table}").fetchall()
    columns = [r[0] for r in described if r[0] != "business_month"]
    fingerprint = f"count(*) AS n, sum(hash(struct_pack({', '.join(columns)}))::HUGEINT) AS h"
    new = {
        m: (n, h)
        for m, n, h in conn.execute(
            f"SELECT business_month, {fingerprint} FROM {table} GROUP BY 1"
        ).fetchall()
    }
    old = {}
    glob = lake.layer_glob(layer, name)
    if glob:
        old = {
            m: (n, h)
            for m, n, h in conn.execute(
                f"SELECT business_month, {fingerprint} FROM read_parquet('{glob}', "
                "hive_partitioning = true, hive_types = {'business_month': VARCHAR}) GROUP BY 1"
            ).fetchall()
        }
    changed = sorted(m for m in new if old.get(m) != new[m])
    for month in changed:
        atomic_copy(
            conn,
            f"SELECT {', '.join(columns)} FROM {table} WHERE business_month = '{month}' "
            f"ORDER BY {order_by}",
            lake.partition_file(layer, name, month),
        )
    for month in sorted(set(old) - set(new)):
        shutil.rmtree(lake.partition_file(layer, name, month).parent)
    return len(changed)


def build_silver(conn, lake: Lake) -> list[EntityStats]:
    """Recompute every silver entity from bronze and sync only the months that changed."""
    stats = []
    for entity in ENTITIES:
        _bronze_view(conn, lake, entity.source)
        conn.execute(
            f"CREATE OR REPLACE TEMP TABLE validated AS {_sql('silver', entity.name + '.sql')}"
        )
        keys = ", ".join(entity.keys)
        conn.execute(
            f"""CREATE OR REPLACE TEMP TABLE silver_new AS
                SELECT * EXCLUDE (reject_reason, _raw, record, _ingest_seq, _arrival_ts),
                       strftime(business_date, '%Y-%m') AS business_month
                FROM validated WHERE reject_reason IS NULL
                QUALIFY row_number() OVER (
                    PARTITION BY {keys} ORDER BY {entity.latest_first}) = 1"""
        )
        conn.execute(
            "CREATE OR REPLACE TEMP TABLE quarantine_new AS "
            + QUARANTINE_SELECT.format(table="validated")
        )
        written = _sync_partitions(conn, lake, "silver", entity.name, "silver_new", keys)
        _sync_partitions(
            conn, lake, "quarantine", entity.source, "quarantine_new", "_file, _record_number"
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
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning = true, "
            "hive_types = {'business_month': VARCHAR})"
        )
    else:
        conn.execute(f"CREATE OR REPLACE TEMP VIEW {view} AS {empty_sql}")


def build_gold(conn, lake: Lake, watermark: date) -> tuple[dict, dict]:
    # Read silver and quarantine back from the lake, so gold reflects persisted state only.
    for entity in ENTITIES:
        _bronze_view(conn, lake, entity.source)
        empty = (
            "SELECT * EXCLUDE (reject_reason, _raw, record, _ingest_seq, _arrival_ts), "
            "'' AS business_month "
            f"FROM ({_sql('silver', entity.name + '.sql')}) WHERE false"
        )
        _lake_view(conn, lake, "silver", entity.name, f"silver_{entity.name}", empty)
    quarantine_parts = []
    for source in SOURCES:
        glob = lake.layer_glob("quarantine", source)
        if glob:
            quarantine_parts.append(
                f"SELECT * FROM read_parquet('{glob}', hive_partitioning = true, "
                "hive_types = {'business_month': VARCHAR})"
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

    conn.execute(
        "CREATE OR REPLACE TEMP VIEW quarantine_facts AS " + _sql("gold", "quarantine_facts.sql")
    )
    lake.file_versions(conn)
    checks, names = [], []
    for header, sql in _sections(_sql("gold", "recon_checks.sql"), "check"):
        name, *options = (part.strip() for part in header.split("|"))
        names.append(name)
        opts = dict(o.split("=", 1) for o in options)
        related = ", ".join(f"'{s.strip()}'" for s in opts["sources"].split(","))
        key = opts["key"]
        key_match = "TRUE" if key == "any" else f"(q.{key} IS NULL OR q.{key} = c.key)"
        # Evidence is searched from window_start (or the row's date) to the row's date, or
        # to the column named by evidence_until (a payment window that runs past the date).
        until = (
            f"coalesce(c.{opts['evidence_until']}, c.business_date)"
            if "evidence_until" in opts
            else "c.business_date"
        )
        checks.append(
            f"""SELECT '{name}' AS check_name, c.business_date, c.key,
                       CAST(c.expected AS DECIMAL(38, 2)) AS expected,
                       CAST(c.actual AS DECIMAL(38, 2)) AS actual,
                       CAST(c.actual - c.expected AS DECIMAL(38, 2)) AS variance,
                       e.*
                FROM ({sql}) c,
                LATERAL (
                    -- Evidence for triage: open quarantined records that might account for
                    -- the variance. They never change the row's status.
                    SELECT count(*) AS candidate_records,
                           list(q._file || '#' || q._record_number || ' ' || q.reject_reason
                                ORDER BY q._file, q._record_number) AS candidates
                    FROM quarantine_facts q
                    WHERE q._source IN ({related}) AND {key_match} AND NOT q.released
                      AND q.business_date
                          BETWEEN coalesce(c.window_start, c.business_date) AND {until}
                ) e"""
        )
    conn.execute(
        f"""CREATE OR REPLACE TEMP TABLE recon_results AS
            SELECT *,
                   CASE WHEN variance = 0 THEN 'matched'
                        WHEN business_date > DATE '{watermark}' - {LATE_ARRIVAL_WINDOW_DAYS}
                            THEN 'open_late_window'
                        ELSE 'exception'
                   END AS status
            FROM ({" UNION ALL ".join(checks)})
            ORDER BY check_name, business_date, key"""
    )
    tables.append("recon_results")
    # What changed since the previous run, keyed on (check, date, key).
    previous = lake.layer_glob("gold", "recon_results")
    prev_sql = (
        f"SELECT check_name, business_date, key, status FROM read_parquet('{previous}')"
        if previous
        else "SELECT NULL::VARCHAR AS check_name, NULL::DATE AS business_date, "
        "NULL::VARCHAR AS key, NULL::VARCHAR AS status WHERE false"
    )
    conn.execute(
        f"""CREATE OR REPLACE TEMP TABLE recon_changes AS
            SELECT coalesce(n.check_name, o.check_name) AS check_name,
                   coalesce(n.business_date, o.business_date) AS business_date,
                   coalesce(n.key, o.key) AS key,
                   o.status AS previous_status, n.status AS status
            FROM recon_results n
            FULL OUTER JOIN ({prev_sql}) o USING (check_name, business_date, key)
            WHERE n.status IS DISTINCT FROM o.status
            ORDER BY ALL"""
    )
    tables.append("recon_changes")
    conn.execute(
        # Every declared check gets a row, so a check that found nothing reads 0 rather
        # than looking as if it never ran.
        f"""CREATE OR REPLACE TEMP TABLE recon_summary AS
           SELECT check_name, count(r.status) AS rows,
                  count(*) FILTER (WHERE status = 'matched') AS matched,
                  count(*) FILTER (WHERE status = 'open_late_window') AS open,
                  count(*) FILTER (WHERE status = 'exception') AS exceptions,
                  count(*) FILTER (WHERE status = 'exception' AND candidate_records > 0)
                      AS exceptions_with_evidence
           FROM (VALUES {", ".join(f"('{n}')" for n in names)}) AS c(check_name)
           LEFT JOIN recon_results r USING (check_name)
           GROUP BY check_name ORDER BY check_name"""
    )
    tables.append("recon_summary")
    conn.execute(
        """CREATE OR REPLACE TEMP TABLE dq_quarantine_summary AS
           SELECT _source AS source, reject_reason, count(*) AS records,
                  count(*) FILTER (WHERE released) AS released_by_later_load
           FROM quarantine_facts GROUP BY ALL ORDER BY ALL"""
    )
    tables.append("dq_quarantine_summary")

    for table in tables:
        atomic_copy(conn, f"SELECT * FROM {table}", lake.gold_file(table))
    summary = {
        name: {"rows": rows, "matched": m, "open": o, "exceptions": x, "with_evidence": ev}
        for name, rows, m, o, x, ev in conn.execute("SELECT * FROM recon_summary").fetchall()
    }
    changes = conn.execute(
        """SELECT count(*) FILTER (WHERE status = 'exception'
                                   AND previous_status IS DISTINCT FROM 'exception'),
                  count(*) FILTER (WHERE previous_status = 'exception'
                                   AND status IS DISTINCT FROM 'exception')
           FROM recon_changes"""
    ).fetchone()
    return summary, {"new_exceptions": changes[0], "cleared_exceptions": changes[1]}


def run_pipeline(landing: Path, lake_root: Path, through: date | None = None) -> RunSummary:
    lake = Lake(lake_root)
    lake_root.mkdir(parents=True, exist_ok=True)
    with lake.lock():
        conn = duckdb.connect()
        try:
            conn.execute("SET TimeZone = 'UTC'")
            conn.execute(MACROS)
            conn.execute(f"SET VARIABLE payment_terms_days = {PAYMENT_TERMS_DAYS}")
            _load_reference(conn)
            batch_id = lake.next_batch_id()
            landed = discover(landing, through)
            results: list[IngestResult] = ingest(conn, lake, landed, batch_id)
            loaded = {r.file.relative for r in results if r.status == "loaded"}
            last_entry = {r["file"]: r for r in lake.ingest_log(conn)}  # ordered by seq
            rejected = [
                (f.relative, last_entry[f.relative]["error"])
                for f in landed
                if last_entry.get(f.relative, {}).get("status") == "rejected"
            ]
            lake.current_versions(conn)
            lost = lake.lost_versions(conn)
            entities = build_silver(conn, lake)
            watermark = lake.watermark(conn) or date.min
            recon, changes = build_gold(conn, lake, watermark)
        finally:
            conn.close()
        lake.write_catalog()
    return RunSummary(
        batch_id=batch_id,
        files_loaded=len(loaded),
        duplicate_files=sum(r.status == "duplicate_content" for r in results),
        rejected_files=rejected,
        lost_files=lost,
        watermark=watermark,
        entities=entities,
        recon=recon,
        changes=changes,
    )
