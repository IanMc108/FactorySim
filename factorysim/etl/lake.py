"""Lake layout, control tables and the query catalog.

    <lake>/bronze/<source>/arrival_date=YYYY-MM-DD/<file>__<sha12>.parquet
    <lake>/silver/<entity>/business_month=YYYY-MM/part-0.parquet
    <lake>/quarantine/<source>/business_month=YYYY-MM/part-0.parquet
    <lake>/gold/<table>/part-0.parquet
    <lake>/_control/ingest_log/batch_id=<id>/batch.parquet   one row per file arrival
    <lake>/catalog.duckdb   views over all of the above (rebuilt every run)

Everything except catalog.duckdb is plain Parquet, so the layout maps directly onto S3
prefixes with the Glue Data Catalog in place of catalog.duckdb.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from .ingest import IngestResult


INGEST_LOG_SCHEMA = pa.schema(
    [
        ("batch_id", pa.string()),
        ("seq", pa.int64()),
        ("source", pa.string()),
        ("file", pa.string()),
        ("sha256", pa.string()),
        ("arrival_date", pa.date32()),
        ("file_business_date", pa.date32()),
        ("status", pa.string()),
        ("error", pa.string()),
        ("encoding", pa.string()),
        ("records", pa.int64()),
        ("header_repeats", pa.int64()),
        ("parse_errors", pa.int64()),
        ("size", pa.int64()),
        ("mtime_ns", pa.int64()),
        ("ingested_at", pa.timestamp("us")),
    ]
)


def _q(path: Path | str) -> str:
    return str(path).replace("'", "''")


def atomic_write_table(table: pa.Table, target: Path) -> None:
    """Write Parquet to a temporary file, then rename it into place."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, target)


def atomic_copy(conn: duckdb.DuckDBPyConnection, select_sql: str, target: Path) -> None:
    """COPY a query result to Parquet via a temporary file and an atomic rename."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    conn.execute(f"COPY ({select_sql}) TO '{_q(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    os.replace(tmp, target)


class Lake:
    def __init__(self, root: Path) -> None:
        self.root = root

    # ------------------------------------------------------------------ paths

    def bronze_file(self, source: str, arrival: date, stem: str, sha: str) -> Path:
        return (
            self.root
            / "bronze"
            / source
            / f"arrival_date={arrival}"
            / f"{stem}__{sha[:12]}.parquet"
        )

    def bronze_glob(self, source: str) -> str | None:
        files = sorted((self.root / "bronze" / source).glob("*/*.parquet"))
        return _q(self.root / "bronze" / source / "*" / "*.parquet") if files else None

    def partition_file(self, layer: str, name: str, month: str) -> Path:
        return self.root / layer / name / f"business_month={month}" / "part-0.parquet"

    def layer_glob(self, layer: str, name: str) -> str | None:
        base = self.root / layer / name
        if not any(base.rglob("*.parquet")):
            return None
        pattern = "*/*.parquet" if layer in ("silver", "quarantine") else "*.parquet"
        return _q(base / pattern)

    def gold_file(self, table: str) -> Path:
        return self.root / "gold" / table / "part-0.parquet"

    # ------------------------------------------------------------------ control

    def _log_glob(self) -> str | None:
        log_dir = self.root / "_control" / "ingest_log"
        return _q(log_dir / "*" / "*.parquet") if any(log_dir.glob("*/*.parquet")) else None

    def ingest_log(self, conn: duckdb.DuckDBPyConnection) -> list[dict]:
        glob = self._log_glob()
        if not glob:
            return []
        cursor = conn.execute(
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning = false, "
            "union_by_name = true) ORDER BY seq"
        )
        names = [d[0] for d in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    def watermark(self, conn: duckdb.DuckDBPyConnection) -> date | None:
        """Latest arrival date of any file the pipeline has seen."""
        glob = self._log_glob()
        if not glob:
            return None
        return conn.execute(
            f"SELECT max(arrival_date) FROM read_parquet('{glob}', hive_partitioning = false)"
        ).fetchone()[0]

    def next_batch_id(self) -> str:
        log_dir = self.root / "_control" / "ingest_log"
        batches = [p for p in log_dir.glob("batch_id=*") if p.is_dir()] if log_dir.exists() else []
        return f"{len(batches) + 1:06d}"

    def write_ingest_log_entry(self, batch_id: str, r: IngestResult) -> None:
        f = r.file
        row = {
            "batch_id": batch_id,
            "seq": r.seq,
            "source": f.source,
            "file": f.relative,
            "sha256": r.sha256,
            "arrival_date": f.arrival_date,
            "file_business_date": f.business_date,
            "status": r.status,
            "error": r.error,
            "encoding": r.encoding,
            "records": r.records,
            "header_repeats": r.header_repeats,
            "parse_errors": r.parse_errors,
            "size": f.size,
            "mtime_ns": f.mtime_ns,
            "ingested_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }
        table = pa.Table.from_pylist([row], schema=INGEST_LOG_SCHEMA)
        target = (
            self.root / "_control" / "ingest_log" / f"batch_id={batch_id}" / f"{r.seq:08d}.parquet"
        )
        atomic_write_table(table, target)

    def compact_ingest_log(self, batch_id: str) -> None:
        """Merge a batch's per-file log entries into one file (they stay valid if we crash)."""
        batch_dir = self.root / "_control" / "ingest_log" / f"batch_id={batch_id}"
        entries = sorted(p for p in batch_dir.glob("*.parquet") if p.name != "batch.parquet")
        if not entries:
            return
        tables = [pq.read_table(p, schema=INGEST_LOG_SCHEMA) for p in entries]
        existing = batch_dir / "batch.parquet"
        if existing.exists():
            tables.insert(0, pq.read_table(existing, schema=INGEST_LOG_SCHEMA))
        atomic_write_table(pa.concat_tables(tables), existing)
        for p in entries:
            p.unlink()

    def current_versions(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create temp table current_versions(source, file_name, sha256) from the log.

        The current version of a file name is its loaded or duplicate entry with the latest
        arrival date, then the latest ingest sequence.
        """
        glob = self._log_glob()
        source = (
            f"read_parquet('{glob}', hive_partitioning = false, union_by_name = true)"
            if glob
            else "(SELECT NULL::VARCHAR AS source, NULL::VARCHAR AS file, NULL::VARCHAR AS sha256, "
            "NULL::DATE AS arrival_date, NULL::BIGINT AS seq, NULL::VARCHAR AS status) WHERE false"
        )
        conn.execute(
            f"""CREATE OR REPLACE TEMP TABLE current_versions AS
                SELECT source, regexp_extract(file, '[^/]+$') AS file_name, sha256
                FROM {source}
                WHERE status IN ('loaded', 'duplicate_content')
                QUALIFY row_number() OVER (
                    PARTITION BY source, regexp_extract(file, '[^/]+$')
                    ORDER BY arrival_date DESC, seq DESC) = 1"""
        )

    def remove_orphan_bronze(self, conn: duckdb.DuckDBPyConnection) -> set[Path]:
        """Delete bronze files with no ingest-log entry (left by a crash mid-file).

        Lists bronze once and returns the bronze files that remain.
        """
        expected = {
            self.bronze_file(r["source"], r["arrival_date"], Path(r["file"]).stem, r["sha256"])
            for r in self.ingest_log(conn)
            if r["status"] == "loaded"
        }
        present = set((self.root / "bronze").glob("*/*/*.parquet"))
        for p in present - expected:
            p.unlink()
        for folder in ("bronze", "_control"):
            for tmp in (self.root / folder).rglob("*.tmp"):
                tmp.unlink()
        return present & expected

    @contextmanager
    def lock(self) -> Iterator[None]:
        """Refuse to run two pipelines against the same lake at once."""
        path = self.root / "_control" / "run.lock"
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise RuntimeError(
                f"{path} exists: another run is in progress, or one crashed "
                "(delete the file once you are sure no run is active)"
            ) from None
        try:
            os.write(fd, f"{os.getpid()}\n".encode())
            os.close(fd)
            yield
        finally:
            path.unlink(missing_ok=True)

    # ------------------------------------------------------------------ catalog

    def write_catalog(self) -> Path:
        """(Re)create catalog.duckdb with one view per lake table."""
        path = self.root / "catalog.duckdb"
        path.unlink(missing_ok=True)
        conn = duckdb.connect(str(path))
        try:
            for schema in ("bronze", "silver", "quarantine", "gold", "control"):
                conn.execute(f"CREATE SCHEMA {schema}")
            for layer, schema in (
                ("bronze", "bronze"),
                ("silver", "silver"),
                ("quarantine", "quarantine"),
                ("gold", "gold"),
            ):
                base = self.root / layer
                if not base.exists():
                    continue
                for table_dir in sorted(p for p in base.iterdir() if p.is_dir()):
                    glob = (
                        self.bronze_glob(table_dir.name)
                        if layer == "bronze"
                        else self.layer_glob(layer, table_dir.name)
                    )
                    if glob:
                        hive = (
                            "true, hive_types = {'business_month': VARCHAR}"
                            if layer in ("silver", "quarantine")
                            else "false"
                        )
                        conn.execute(
                            f"CREATE VIEW {schema}.{table_dir.name} AS "
                            f"SELECT * FROM read_parquet('{glob}', hive_partitioning = {hive})"
                        )
            log = self._log_glob()
            if log:
                conn.execute(
                    "CREATE VIEW control.ingest_log AS "
                    f"SELECT * FROM read_parquet('{log}', hive_partitioning = false)"
                )
        finally:
            conn.close()
        return path
