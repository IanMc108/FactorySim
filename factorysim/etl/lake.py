"""Lake layout, control tables and the query catalog.

    <lake>/bronze/<source>/arrival_date=YYYY-MM-DD/<file>__<sha12>.parquet
    <lake>/silver/<entity>/business_month=YYYY-MM/part-0.parquet
    <lake>/quarantine/<source>/business_month=YYYY-MM/part-0.parquet
    <lake>/gold/<table>/part-0.parquet
    <lake>/_control/ingest_log/batch_id=<id>.parquet
    <lake>/catalog.duckdb   views over all of the above (rebuilt every run)

Everything except catalog.duckdb is plain Parquet, so the layout maps directly onto S3
prefixes with the Glue Data Catalog in place of catalog.duckdb.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from .ingest import IngestResult


def _q(path: Path | str) -> str:
    return str(path).replace("'", "''")


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
        return _q(log_dir / "*.parquet") if any(log_dir.glob("*.parquet")) else None

    def loaded_hashes(self, conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
        glob = self._log_glob()
        if not glob:
            return {}
        rows = conn.execute(
            f"SELECT sha256, file FROM read_parquet('{glob}') WHERE status = 'loaded'"
        ).fetchall()
        return dict(rows)

    def seen_files(self, conn: duckdb.DuckDBPyConnection) -> set[str]:
        glob = self._log_glob()
        if not glob:
            return set()
        return {r[0] for r in conn.execute(f"SELECT file FROM read_parquet('{glob}')").fetchall()}

    def watermark(self, conn: duckdb.DuckDBPyConnection) -> date | None:
        """Latest arrival date of any file the pipeline has seen."""
        glob = self._log_glob()
        if not glob:
            return None
        return conn.execute(f"SELECT max(arrival_date) FROM read_parquet('{glob}')").fetchone()[0]

    def next_batch_id(self) -> str:
        log_dir = self.root / "_control" / "ingest_log"
        return f"{len(list(log_dir.glob('*.parquet'))) + 1:06d}" if log_dir.exists() else "000001"

    def append_ingest_log(
        self, conn: duckdb.DuckDBPyConnection, batch_id: str, results: list[IngestResult]
    ) -> None:
        if not results:
            return  # nothing new arrived; a no-op run leaves no trace
        log_dir = self.root / "_control" / "ingest_log"
        log_dir.mkdir(parents=True, exist_ok=True)
        conn.execute(
            """CREATE OR REPLACE TEMP TABLE _log (
                   batch_id VARCHAR, source VARCHAR, file VARCHAR, sha256 VARCHAR,
                   arrival_date DATE, file_business_date DATE, status VARCHAR,
                   encoding VARCHAR, records INTEGER, header_repeats INTEGER,
                   parse_errors INTEGER, ingested_at TIMESTAMP)"""
        )
        conn.executemany(
            "INSERT INTO _log VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp::TIMESTAMP)",
            [
                (
                    batch_id,
                    r.file.source,
                    r.file.relative,
                    r.sha256,
                    r.file.arrival_date,
                    r.file.business_date,
                    r.status,
                    r.encoding,
                    r.records,
                    r.header_repeats,
                    r.parse_errors,
                )
                for r in results
            ],
        )
        conn.execute(
            f"COPY _log TO '{_q(log_dir / f'batch_id={batch_id}.parquet')}' (FORMAT PARQUET)"
        )

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
                        conn.execute(
                            f"CREATE VIEW {schema}.{table_dir.name} AS "
                            f"SELECT * FROM read_parquet('{glob}', hive_partitioning = false)"
                        )
            log = self._log_glob()
            if log:
                conn.execute(
                    f"CREATE VIEW control.ingest_log AS SELECT * FROM read_parquet('{log}')"
                )
        finally:
            conn.close()
        return path
