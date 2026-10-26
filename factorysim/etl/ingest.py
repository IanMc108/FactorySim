"""Landing -> bronze: load each new file as-is, with lineage, exactly once.

Bronze keeps every record of every file the pipeline has accepted, untyped, so any later
layer can be rebuilt from it. Each record carries its source file, the file's SHA-256,
the arrival and business dates, and its record number in the file. Records that cannot
be split into the header's columns are kept with a ``_parse_error`` rather than dropped.

Idempotency: a file whose content hash is already in the ingest log is never loaded
twice, whether it reappears at the same path (a re-run) or a new one (a re-delivery).
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .lake import Lake

SOURCES = {
    "mes_production": "csv",
    "mes_material_issues": "csv",
    "erp_invoices": "csv",
    "erp_goods_receipts": "csv",
    "erp_stock_snapshots": "csv",
    "cmms_work_orders": "jsonl",
    "bank_statements": "csv",
}
_FILE_DATE = re.compile(r"_(\d{8})\.(csv|jsonl)$")
_ARRIVAL = re.compile(r"(\d{4})/(\d{2})/(\d{2})$")

BRONZE_SCHEMA = pa.schema(
    [
        ("_source", pa.string()),
        ("_file", pa.string()),
        ("_file_sha256", pa.string()),
        ("_arrival_date", pa.date32()),
        ("_file_business_date", pa.date32()),
        ("_record_number", pa.int32()),
        ("_batch_id", pa.string()),
        ("_parse_error", pa.string()),
        ("_raw", pa.string()),
        ("record", pa.string()),
    ]
)
# The same layout as SQL, for empty bronze relations.
BRONZE_COLUMNS = (
    "_source VARCHAR, _file VARCHAR, _file_sha256 VARCHAR, _arrival_date DATE, "
    "_file_business_date DATE, _record_number INTEGER, _batch_id VARCHAR, "
    "_parse_error VARCHAR, _raw VARCHAR, record VARCHAR"
)


@dataclass(frozen=True)
class LandedFile:
    source: str
    path: Path
    relative: str
    arrival_date: date
    business_date: date


@dataclass(frozen=True)
class IngestResult:
    file: LandedFile
    sha256: str
    status: str  # loaded | duplicate_content
    encoding: str
    records: int
    header_repeats: int
    parse_errors: int


def discover(landing: Path, through: date | None = None) -> list[LandedFile]:
    """Landed files in arrival order, optionally only those arrived on or before ``through``."""
    found = []
    for source in SOURCES:
        root = landing / source
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            arrival = _ARRIVAL.search(path.parent.relative_to(landing).as_posix())
            name_date = _FILE_DATE.search(path.name)
            if not arrival or not name_date:
                raise ValueError(f"unexpected file in landing zone: {path}")
            arrival_date = date(*map(int, arrival.groups()))
            if through and arrival_date > through:
                continue
            business = name_date.group(1)
            found.append(
                LandedFile(
                    source,
                    path,
                    path.relative_to(landing).as_posix(),
                    arrival_date,
                    date(int(business[:4]), int(business[4:6]), int(business[6:])),
                )
            )
    return sorted(found, key=lambda f: (f.arrival_date, f.source, f.relative))


def _decode(payload: bytes) -> tuple[str, str]:
    try:
        return payload.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        return payload.decode("cp1252"), "cp1252"


def _parse_csv(text: str) -> tuple[list[tuple], int, int]:
    reader = csv.reader(io.StringIO(text, newline=""))
    header = None
    rows, repeats, errors = [], 0, 0
    for number, fields in enumerate(reader, start=1):
        if header is None:
            header = [h.strip() for h in fields]
            continue
        if not fields:
            continue
        if [f.strip() for f in fields] == header:
            repeats += 1
            continue
        raw = json.dumps(fields, ensure_ascii=False)
        if len(fields) != len(header):
            errors += 1
            rows.append((number, f"column_count:{len(fields)}/{len(header)}", raw, None))
            continue
        rows.append((number, None, raw, json.dumps(dict(zip(header, fields, strict=True)))))
    return rows, repeats, errors


def _parse_jsonl(text: str) -> tuple[list[tuple], int, int]:
    rows, errors = [], 0
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError("not an object")
        except ValueError:
            errors += 1
            rows.append((number, "invalid_json", line, None))
            continue
        rows.append((number, None, line, json.dumps(obj, ensure_ascii=False)))
    return rows, 0, errors


def ingest(
    conn: duckdb.DuckDBPyConnection, lake: Lake, files: list[LandedFile], batch_id: str
) -> list[IngestResult]:
    """Load new files into bronze and append their outcome to the ingest log."""
    known, seen = lake.loaded_hashes(conn), lake.seen_files(conn)
    results = []
    for f in files:
        if f.relative in seen:
            continue  # handled by an earlier run
        seen.add(f.relative)
        payload = f.path.read_bytes()
        sha = hashlib.sha256(payload).hexdigest()
        if sha in known:
            results.append(IngestResult(f, sha, "duplicate_content", "", 0, 0, 0))
            continue
        text, encoding = _decode(payload)
        parse = _parse_csv if SOURCES[f.source] == "csv" else _parse_jsonl
        rows, repeats, errors = parse(text)
        target = lake.bronze_file(f.source, f.arrival_date, Path(f.relative).stem, sha)
        target.parent.mkdir(parents=True, exist_ok=True)
        columns = list(zip(*rows, strict=True)) if rows else [(), (), (), ()]
        n = len(rows)
        table = pa.table(
            {
                "_source": pa.array([f.source] * n, pa.string()),
                "_file": pa.array([f.relative] * n, pa.string()),
                "_file_sha256": pa.array([sha] * n, pa.string()),
                "_arrival_date": pa.array([f.arrival_date] * n, pa.date32()),
                "_file_business_date": pa.array([f.business_date] * n, pa.date32()),
                "_record_number": pa.array(columns[0], pa.int32()),
                "_batch_id": pa.array([batch_id] * n, pa.string()),
                "_parse_error": pa.array(columns[1], pa.string()),
                "_raw": pa.array(columns[2], pa.string()),
                "record": pa.array(columns[3], pa.string()),
            },
            schema=BRONZE_SCHEMA,
        )
        pq.write_table(table, target, compression="zstd")
        known[sha] = f.relative
        results.append(IngestResult(f, sha, "loaded", encoding, len(rows), repeats, errors))
    lake.append_ingest_log(conn, batch_id, results)
    return results
