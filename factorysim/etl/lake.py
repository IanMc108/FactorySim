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

import json
import logging
import os
import socket
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

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
        ("arrival_ts", pa.timestamp("us")),
        ("file_business_date", pa.date32()),
        ("status", pa.string()),
        ("error", pa.string()),
        ("encoding", pa.string()),
        ("records", pa.int64()),
        ("header_repeats", pa.int64()),
        ("parse_errors", pa.int64()),
        # Fingerprint of the parsed records, so the same data in different bytes (CRLF line
        # endings, a BOM, another encoding) is recognised as the same version's content.
        ("content_sha256", pa.string()),
        ("size", pa.int64()),
        ("mtime_ns", pa.int64()),
        ("ctime_ns", pa.int64()),
        ("ingested_at", pa.timestamp("us")),
    ]
)


class LakeLocked(RuntimeError):
    """Another run holds the lake's lock."""


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    return True


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


# The order of a file name's versions, newest first. One arrival folder holds one path per
# file name, so within a folder the latest log entry (by sequence) is what is on disk now,
# even after an overwrite by a copy with an older modification time. Used by everything
# that ranks versions, so the current version and the shrink/revert checks always agree.
VERSION_ORDER = "arrival_date DESC, seq DESC"


class Lake:
    def __init__(self, root: Path) -> None:
        self.root = root

    # ------------------------------------------------------------------ paths

    def bronze_file(
        self, source: str, arrival: date, stem: str, sha: str, duplicate: bool = False
    ) -> Path:
        # A duplicate arrival gets an empty marker file: it holds no records (the loaded
        # version has them) but carries the arrival's ingest metadata, so the log entry can
        # be rebuilt from bronze like any other.
        return (
            self.root
            / "bronze"
            / source
            / f"arrival_date={arrival}"
            / f"{stem}__{sha[:12]}{'.dup' if duplicate else ''}.parquet"
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
        """Latest arrival date of any file the pipeline accepted (never a rejected one)."""
        glob = self._log_glob()
        if not glob:
            return None
        return conn.execute(
            f"SELECT max(arrival_date) FROM read_parquet('{glob}', hive_partitioning = false, "
            "union_by_name = true) WHERE status IN ('loaded', 'duplicate_content', 'touched')"
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
            "arrival_ts": f.arrival_ts,
            "file_business_date": f.business_date,
            "status": r.status,
            "error": r.error,
            "encoding": r.encoding,
            "records": r.records,
            "header_repeats": r.header_repeats,
            "parse_errors": r.parse_errors,
            "content_sha256": r.content_sha256,
            "size": f.size,
            "mtime_ns": f.mtime_ns,
            "ctime_ns": f.ctime_ns,
            "ingested_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }
        table = pa.Table.from_pylist([row], schema=INGEST_LOG_SCHEMA)
        target = (
            self.root / "_control" / "ingest_log" / f"batch_id={batch_id}" / f"{r.seq:08d}.parquet"
        )
        atomic_write_table(table, target)

    def entry_bronze(self, r: dict) -> Path:
        """The bronze file a loaded or duplicate log entry wrote."""
        return self.bronze_file(
            r["source"],
            r["arrival_date"],
            Path(r["file"]).stem,
            r["sha256"],
            duplicate=r["status"] == "duplicate_content",
        )

    def bronze_files(self) -> set[Path]:
        return set((self.root / "bronze").glob("*/*/*.parquet"))

    @staticmethod
    def content_key(source: str, file: str, sha: str) -> tuple[str, str, str]:
        """Identifies a file version's records in bronze, whichever arrival wrote them."""
        return source, Path(file).stem, sha[:12]

    def bronze_contents(self, present: set[Path]) -> set[tuple[str, str, str]]:
        """Content keys of the file versions whose records are in bronze (not markers)."""
        return {
            (p.parts[-3], *p.name.removesuffix(".parquet").rsplit("__", 1))
            for p in present
            if not p.name.endswith(".dup.parquet")
        }

    def lost_versions(self, conn: duckdb.DuckDBPyConnection) -> list[str]:
        """Current file versions whose records are in no bronze file (after ingest has
        rebuilt what it could from the landing zone): they are no longer in silver."""
        contents = self.bronze_contents(self.bronze_files())
        lost = {
            (s, n, h)
            for s, n, h in conn.execute("SELECT * FROM current_versions").fetchall()
            if self.content_key(s, n, h) not in contents
        }
        return sorted(
            {
                r["file"]
                for r in self.ingest_log(conn)
                if r["status"] == "loaded"
                and (r["source"], Path(r["file"]).name, r["sha256"]) in lost
            }
        )

    def repair_ingest_log(self) -> None:
        """Finish a compaction that crashed after writing batch.parquet but before removing
        the per-file entries it replaced, so no entry is ever counted twice."""
        for batch_dir in sorted((self.root / "_control" / "ingest_log").glob("batch_id=*")):
            compacted = batch_dir / "batch.parquet"
            leftovers = [p for p in batch_dir.glob("*.parquet") if p != compacted]
            if not leftovers or not compacted.exists():
                continue
            done = set(pq.read_table(compacted, columns=["seq"]).column("seq").to_pylist())
            for p in leftovers:
                if set(pq.read_table(p, columns=["seq"]).column("seq").to_pylist()) <= done:
                    p.unlink()
            self.compact_ingest_log(batch_dir.name.removeprefix("batch_id="))

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

        The current version of a file name is its loaded or duplicate entry that comes first
        in VERSION_ORDER.
        """
        glob = self._log_glob()
        source = (
            f"read_parquet('{glob}', hive_partitioning = false, union_by_name = true)"
            if glob
            else "(SELECT NULL::VARCHAR AS source, NULL::VARCHAR AS file, NULL::VARCHAR AS sha256, "
            "NULL::DATE AS arrival_date, NULL::TIMESTAMP AS arrival_ts, NULL::BIGINT AS seq, "
            "NULL::VARCHAR AS status WHERE false)"
        )
        conn.execute(
            f"""CREATE OR REPLACE TEMP TABLE current_versions AS
                SELECT source, regexp_extract(file, '[^/]+$') AS file_name, sha256
                FROM {source}
                WHERE status IN ('loaded', 'duplicate_content')
                QUALIFY row_number() OVER (
                    PARTITION BY source, regexp_extract(file, '[^/]+$')
                    ORDER BY {VERSION_ORDER}) = 1"""
        )

    def file_versions(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create temp view file_versions: per file name, its versions and record counts."""
        glob = self._log_glob()
        if not glob:
            conn.execute(
                """CREATE OR REPLACE TEMP VIEW file_versions AS
                   SELECT NULL::VARCHAR AS source, NULL::VARCHAR AS file_name,
                          NULL::DATE AS file_business_date, NULL::BIGINT AS versions,
                          NULL::BIGINT AS current_records, NULL::BIGINT AS max_earlier_records,
                          NULL::BOOLEAN AS reverted
                   WHERE false"""
            )
            return
        conn.execute(
            f"""CREATE OR REPLACE TEMP VIEW file_versions AS
                WITH log AS (
                    SELECT *, regexp_extract(file, '[^/]+$') AS file_name
                    FROM read_parquet('{glob}', hive_partitioning = false, union_by_name = true)
                    WHERE status IN ('loaded', 'duplicate_content')
                ),
                content AS (
                    SELECT source, file_name, sha256, max(records) AS records,
                           any_value(coalesce(content_sha256, sha256)) AS content
                    FROM log WHERE status = 'loaded' GROUP BY source, file_name, sha256
                ),
                ranked AS (
                    SELECT l.source, l.file_name, l.file_business_date, c.content, c.records,
                           row_number() OVER (
                               PARTITION BY l.source, l.file_name ORDER BY {VERSION_ORDER}
                           ) AS rn,
                           count(*) OVER (PARTITION BY l.source, l.file_name) AS versions
                    FROM log l JOIN content c USING (source, file_name, sha256)
                ),
                current AS (SELECT source, file_name, content FROM ranked WHERE rn = 1)
                SELECT r.source, r.file_name,
                       any_value(r.file_business_date) AS file_business_date,
                       max(r.versions) AS versions,
                       max(r.records) FILTER (WHERE r.rn = 1) AS current_records,
                       coalesce(max(r.records) FILTER (WHERE r.rn > 1), 0) AS max_earlier_records,
                       -- A revert: the current records had been replaced by different records
                       -- before they came back (A, B, then A again, e.g. a stale re-send, in
                       -- the same or different bytes). rn grows with age, so some copy of A
                       -- is older than the newest non-A.
                       coalesce(min(r.rn) FILTER (WHERE r.content <> cur.content)
                                < max(r.rn) FILTER (WHERE r.rn > 1 AND r.content = cur.content),
                                false) AS reverted
                FROM ranked r JOIN current cur USING (source, file_name)
                GROUP BY r.source, r.file_name"""
        )

    def recover_orphan_bronze(self, conn: duckdb.DuckDBPyConnection) -> set[Path]:
        """Re-register bronze files that have no ingest-log entry; never delete bronze.

        An orphan is left by a crash between writing a bronze file and logging it, or by a
        lost control table. Every bronze file carries its ingest metadata, so its log entry
        is rebuilt from the file itself. A file without readable metadata is moved to
        ``_orphaned/`` for a person to look at. Returns the bronze files now in the log.
        """
        self.repair_ingest_log()
        expected = {
            self.entry_bronze(r)
            for r in self.ingest_log(conn)
            if r["status"] in ("loaded", "duplicate_content")
        }
        present = self.bronze_files()
        for orphan in sorted(present - expected):
            meta = (pq.read_schema(orphan).metadata or {}).get(b"factorysim.ingest")
            if meta is None:
                target = self.root / "_orphaned" / orphan.relative_to(self.root)
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(orphan, target)
                present.discard(orphan)
                continue
            m = json.loads(meta)
            row = {
                "batch_id": m["batch_id"],
                "seq": m["seq"],
                "source": m["source"],
                "file": m["file"],
                "sha256": m["sha256"],
                "arrival_date": date.fromisoformat(m["arrival_date"]),
                "arrival_ts": datetime.fromisoformat(m["arrival_ts"]),
                "file_business_date": date.fromisoformat(m["file_business_date"]),
                "status": m.get("status", "loaded"),
                "error": "recovered from bronze metadata",
                "encoding": m["encoding"],
                "records": m["records"],
                "header_repeats": m["header_repeats"],
                "parse_errors": m["parse_errors"],
                "content_sha256": m.get("content_sha256"),
                "size": m["size"],
                "mtime_ns": m["mtime_ns"],
                "ctime_ns": m["ctime_ns"],
                "ingested_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
            atomic_write_table(
                pa.Table.from_pylist([row], schema=INGEST_LOG_SCHEMA),
                self.root
                / "_control"
                / "ingest_log"
                / f"batch_id={m['batch_id']}"
                / f"{m['seq']:08d}.parquet",
            )
        for folder in ("bronze", "_control"):
            for tmp in (self.root / folder).rglob("*.tmp"):
                tmp.unlink()
        return present

    @contextmanager
    def lock(self) -> Iterator[None]:
        """Refuse to run two pipelines against the same lake at once.

        The lock file holds the owner's host and PID. A lock left by a process that no
        longer exists on this host (killed by the OOM killer, a spot interruption) is
        taken over; any other lock is respected.
        """
        path = self.root / "_control" / "run.lock"
        path.parent.mkdir(parents=True, exist_ok=True)
        host = socket.gethostname()
        for _ in range(2):
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                break
            except FileExistsError:
                owner_host, _, pid = path.read_text().strip().partition(" ")
                if owner_host != host or not pid.isdigit() or _process_alive(int(pid)):
                    raise LakeLocked(
                        f"{path} is held by {owner_host or '?'} pid {pid or '?'}: another run "
                        "is in progress (delete the file only if that process is gone)"
                    ) from None
                log.warning("taking over %s from dead process %s", path, pid)
                path.unlink(missing_ok=True)
        else:
            raise LakeLocked(f"{path}: could not take the lock")
        try:
            os.write(fd, f"{host} {os.getpid()}\n".encode())
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
