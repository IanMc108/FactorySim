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
MAX_OPTIONAL_EFFECTS = 12
STATUSES = ("matched", "explained_by_quarantine", "open_late_window", "needs_review", "exception")


@dataclass(frozen=True)
class Entity:
    name: str
    source: str
    keys: tuple[str, ...]
    # Among duplicates of one key, the first row in this order wins: the newest extract
    # (file business date), then the most recently ingested file, then the last record.
    latest_first: str = "_file_business_date DESC, _ingest_seq DESC, _record_number DESC"


ENTITIES = (
    Entity(
        "production_events",
        "mes_production",
        ("event_id",),
        "revision DESC, _file_business_date DESC, _ingest_seq DESC, _record_number DESC",
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
-- True if req plus some subset of opts equals target. Callers keep opts to at most
-- MAX_OPTIONAL_EFFECTS elements; a longer list is never "explained" (see build_gold).
CREATE OR REPLACE MACRO subset_sum_matches(req, opts, target) AS
    CASE WHEN opts IS NULL OR len(opts) = 0 THEN req = target
         WHEN len(opts) > 12 THEN false
         ELSE len(list_filter(range(0, 1 << len(opts)), lambda m:
              req + list_sum(list_transform(range(len(opts)), lambda i:
                  CASE WHEN ((m >> i) & 1) = 1 THEN opts[i + 1] ELSE 0 END)) = target)) > 0
    END;
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
CREATE OR REPLACE MACRO money(s) AS
    CASE WHEN regexp_full_match(regexp_replace(s, '[$,\s]', '', 'g'), '-?[0-9]+(\.[0-9]{1,4})?')
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
    rejected_files: list[tuple[str, str | None]]
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
                SELECT * EXCLUDE (reject_reason, _raw, record, _ingest_seq),
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


def build_gold(conn, lake: Lake, watermark: date) -> dict[str, dict[str, int]]:
    # Read silver and quarantine back from the lake, so gold reflects persisted state only.
    for entity in ENTITIES:
        _bronze_view(conn, lake, entity.source)
        empty = (
            f"SELECT * EXCLUDE (reject_reason, _raw, record, _ingest_seq), '' AS business_month "
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
    checks = []
    for header, sql in _sections(_sql("gold", "recon_checks.sql"), "check"):
        name, *options = (part.strip() for part in header.split("|"))
        opts = dict(o.split("=", 1) for o in options)
        # effect=<feed>:<effect>, ... : how losing one record of that feed moves the variance
        # (actual - expected). <effect> is +measure or -measure (e.g. +units), +? or -? when
        # only the direction is known, or ? when even that is not.
        effects = [e.strip().split(":") for e in opts["effect"].split(",")]
        related = ", ".join(f"'{feed}'" for feed, _ in effects)

        def size_sql(eff: str) -> str:
            if eff.endswith("?"):
                return "NULL"
            return f"{'-' if eff[0] == '-' else ''}q.{eff[1:]}"

        def direction(eff: str) -> int:
            return {"+": 1, "-": -1}.get(eff[0], 0) if eff != "?" else 0

        def bound_sql(eff: str) -> str:
            return "q.units_bound" if eff[1:] == "units_recovered" else "NULL"

        size = " ".join(f"WHEN q._source = '{f}' THEN {size_sql(e)}" for f, e in effects)
        bound = " ".join(f"WHEN q._source = '{f}' THEN {bound_sql(e)}" for f, e in effects)
        sign = " ".join(f"WHEN q._source = '{f}' THEN {direction(e)}" for f, e in effects)
        key = opts["key"]
        key_match = "TRUE" if key == "any" else f"(q.{key} IS NULL OR q.{key} = c.key)"
        # A record whose key is known certainly belongs to this row; one whose key could not
        # be recovered may or may not.
        key_known = "TRUE" if key == "any" else f"q.{key} IS NOT NULL"
        related_rows = f"""FROM quarantine_facts q
                           WHERE q._source IN ({related}) AND {key_match} AND NOT q.released
                             AND q.business_date
                                 BETWEEN coalesce(c.window_start, c.business_date)
                                     AND c.business_date"""
        checks.append(
            f"""SELECT '{name}' AS check_name, c.business_date, c.key,
                       CAST(c.expected AS DECIMAL(38, 2)) AS expected,
                       CAST(c.actual AS DECIMAL(38, 2)) AS actual,
                       CAST(c.actual - c.expected AS DECIMAL(38, 2)) AS variance,
                       e.*
                FROM ({sql}) c,
                LATERAL (
                    SELECT count(*) AS related_quarantined,
                           -- certainly (known key), possibly (unknown key), or of unknown size
                           coalesce(sum(x) FILTER (WHERE known AND x IS NOT NULL), 0)
                               AS quarantine_effect,
                           list(x) FILTER (WHERE NOT known AND x IS NOT NULL)
                               AS possible_effects,
                           count(*) FILTER (WHERE x IS NULL) AS unsized_records,
                           list(DISTINCT d) FILTER (WHERE x IS NULL) AS unsized_directions,
                           -- the most the uncertain records could move the variance;
                           -- NULL if some unsized record has no bound at all
                           CASE WHEN count(*) FILTER (WHERE x IS NULL AND b IS NULL) > 0
                                THEN NULL
                                ELSE coalesce(sum(b) FILTER (WHERE x IS NULL), 0)
                                     + coalesce(sum(abs(x)) FILTER (WHERE NOT known), 0)
                           END AS uncertainty_bound
                    FROM (SELECT CASE {size} END AS x, CASE {sign} END AS d,
                                 CASE {bound} END AS b, {key_known} AS known
                          {related_rows})
                ) e"""
        )
    conn.execute(
        f"""CREATE OR REPLACE TEMP TABLE recon_results AS
            SELECT *,
                   CASE WHEN variance = 0 THEN 'matched'
                        -- Explained only if it adds up exactly.
                        WHEN related_quarantined > 0 AND unsized_records = 0
                             AND coalesce(len(possible_effects), 0) <= {MAX_OPTIONAL_EFFECTS}
                             AND subset_sum_matches(quarantine_effect, possible_effects,
                                                    variance)
                            THEN 'explained_by_quarantine'
                        WHEN business_date > DATE '{watermark}' - {LATE_ARRIVAL_WINDOW_DAYS}
                            THEN 'open_late_window'
                        -- Plausibly caused by quarantined records whose size is unknown (but
                        -- whose direction and bound fit), or by too many candidates to check:
                        -- a person has to look. Never silently "explained".
                        WHEN related_quarantined > 0
                             AND ((unsized_records > 0
                                   AND (list_contains(unsized_directions, 0)
                                        OR list_contains(unsized_directions,
                                                         sign(variance - quarantine_effect))))
                                  OR len(possible_effects) > {MAX_OPTIONAL_EFFECTS})
                             AND (uncertainty_bound IS NULL
                                  OR abs(variance - quarantine_effect) <= uncertainty_bound)
                            THEN 'needs_review'
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
                  count(*) FILTER (WHERE status = 'needs_review') AS needs_review,
                  count(*) FILTER (WHERE status = 'exception') AS exceptions
           FROM recon_results GROUP BY check_name ORDER BY check_name"""
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
    return {
        name: {
            "rows": rows,
            "matched": m,
            "explained": e,
            "open": o,
            "needs_review": r,
            "exceptions": x,
        }
        for name, rows, m, e, o, r, x in conn.execute("SELECT * FROM recon_summary").fetchall()
    }


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
            results: list[IngestResult] = ingest(conn, lake, discover(landing, through), batch_id)
            loaded = {r.file.relative for r in results if r.status == "loaded"}
            lake.current_versions(conn)
            entities = build_silver(conn, lake)
            watermark = lake.watermark(conn) or date.min
            recon = build_gold(conn, lake, watermark)
        finally:
            conn.close()
        lake.write_catalog()
    return RunSummary(
        batch_id=batch_id,
        files_loaded=len(loaded),
        duplicate_files=sum(r.status == "duplicate_content" for r in results),
        rejected_files=[(r.file.relative, r.error) for r in results if r.status == "rejected"],
        watermark=watermark,
        entities=entities,
        recon=recon,
    )
