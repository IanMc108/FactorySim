"""Landing -> bronze: load each new file version as-is, with lineage, exactly once.

Bronze keeps every record of every file version the pipeline has accepted, untyped, so
any later layer can be rebuilt from it. Each record carries its source file, the file's
SHA-256, the arrival and business dates, the file's ingest sequence number and its record
number in the file. Records that cannot be split into the header's columns are kept with a
``_parse_error`` (and their raw text) rather than dropped.

A file *version* is a (path, SHA-256) pair:

* the same path with the same content again (a re-run) is skipped silently;
* the same content at a new path (a re-delivery) is logged as ``duplicate_content``;
* new content at a known path (an in-place overwrite) is loaded as a new version, and
  silver's latest-wins ordering makes it supersede the old one;
* a file that cannot be read (bad name, undecodable bytes) is logged as ``rejected``
  and the rest of the batch carries on. Marker files such as ``_SUCCESS`` are ignored.

Each file is committed on its own: its bronze Parquet is written to a temporary name and
renamed, then its ingest-log entry is written the same way. A crash between the two leaves
an orphan bronze file, which the next run deletes before doing anything else.
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

from .lake import Lake, atomic_write_table

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
_ARRIVAL = re.compile(r"^[^/]+/(\d{4})/(\d{2})/(\d{2})$")

BRONZE_SCHEMA = pa.schema(
    [
        ("_source", pa.string()),
        ("_file", pa.string()),
        ("_file_sha256", pa.string()),
        ("_arrival_date", pa.date32()),
        ("_file_business_date", pa.date32()),
        ("_ingest_seq", pa.int64()),
        ("_record_number", pa.int32()),
        ("_batch_id", pa.string()),
        ("_parse_error", pa.string()),
        ("_raw", pa.string()),
        ("record", pa.string()),
    ]
)
# The same layout in SQL, for empty bronze relations.
BRONZE_COLUMNS = (
    "_source VARCHAR, _file VARCHAR, _file_sha256 VARCHAR, _arrival_date DATE, "
    "_file_business_date DATE, _ingest_seq BIGINT, _record_number INTEGER, "
    "_batch_id VARCHAR, _parse_error VARCHAR, _raw VARCHAR, record VARCHAR"
)


@dataclass(frozen=True)
class LandedFile:
    source: str
    path: Path
    relative: str
    arrival_date: date | None
    business_date: date | None


@dataclass(frozen=True)
class IngestResult:
    file: LandedFile
    sha256: str
    seq: int
    status: str  # loaded | duplicate_content | rejected
    encoding: str
    records: int
    header_repeats: int
    parse_errors: int
    error: str | None = None


def _ymd(text: str) -> date | None:
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except ValueError:
        return None


def discover(landing: Path, through: date | None = None) -> list[LandedFile]:
    """Landed files in arrival order, optionally only those arrived on or before ``through``.

    Files whose name or folder does not follow the landing convention are still returned
    (with missing dates) so that ingestion can log them as rejected.
    """
    found = []
    for source in SOURCES:
        root = landing / source
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.name.startswith(("_", ".")):
                continue
            relative = path.relative_to(landing).as_posix()
            arrival_match = _ARRIVAL.search(path.parent.relative_to(landing).as_posix())
            arrival = _ymd("".join(arrival_match.groups())) if arrival_match else None
            if through and arrival and arrival > through:
                continue
            name_match = _FILE_DATE.search(path.name)
            business = _ymd(name_match.group(1)) if name_match else None
            found.append(LandedFile(source, path, relative, arrival, business))
    return sorted(found, key=lambda f: (f.arrival_date or date.min, f.source, f.relative))


def _decode(payload: bytes) -> tuple[str, str]:
    try:
        return payload.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        return payload.decode("cp1252"), "cp1252"  # raises if not valid cp1252 either


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
        if len(fields) != len(header):
            errors += 1
            raw = json.dumps(fields, ensure_ascii=False)
            # A short (truncated) row keeps its leading columns; map them by position so
            # reconciliation can still see e.g. the station or SKU. It stays a parse error.
            partial = (
                json.dumps(dict(zip(header, fields, strict=False)), ensure_ascii=False)
                if len(fields) < len(header)
                else None
            )
            rows.append((number, f"column_count:{len(fields)}/{len(header)}", raw, partial))
            continue
        record = json.dumps(dict(zip(header, fields, strict=True)), ensure_ascii=False)
        rows.append((number, None, None, record))
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
        rows.append((number, None, None, json.dumps(obj, ensure_ascii=False)))
    return rows, 0, errors


def _bronze_table(f: LandedFile, sha: str, seq: int, batch_id: str, rows: list[tuple]):
    columns = list(zip(*rows, strict=True)) if rows else [(), (), (), ()]
    n = len(rows)
    return pa.table(
        {
            "_source": pa.array([f.source] * n, pa.string()),
            "_file": pa.array([f.relative] * n, pa.string()),
            "_file_sha256": pa.array([sha] * n, pa.string()),
            "_arrival_date": pa.array([f.arrival_date] * n, pa.date32()),
            "_file_business_date": pa.array([f.business_date] * n, pa.date32()),
            "_ingest_seq": pa.array([seq] * n, pa.int64()),
            "_record_number": pa.array(columns[0], pa.int32()),
            "_batch_id": pa.array([batch_id] * n, pa.string()),
            "_parse_error": pa.array(columns[1], pa.string()),
            "_raw": pa.array(columns[2], pa.string()),
            "record": pa.array(columns[3], pa.string()),
        },
        schema=BRONZE_SCHEMA,
    )


def ingest(
    conn: duckdb.DuckDBPyConnection, lake: Lake, files: list[LandedFile], batch_id: str
) -> list[IngestResult]:
    """Load new file versions into bronze, committing each file and its log entry in turn."""
    lake.remove_orphan_bronze(conn)
    log = lake.ingest_log(conn)
    # A loaded version whose bronze file has gone missing is treated as never seen, so the
    # landing file is ingested again rather than silently dropping out of silver.
    lost = {
        (r["file"], r["sha256"])
        for r in log
        if r["status"] == "loaded"
        and not lake.bronze_file(
            r["source"], r["arrival_date"], Path(r["file"]).stem, r["sha256"]
        ).exists()
    }
    loaded_hashes = {
        r["sha256"] for r in log if r["status"] == "loaded" and (r["file"], r["sha256"]) not in lost
    }
    seen_versions = {(r["file"], r["sha256"]) for r in log} - lost
    next_seq = max((r["seq"] for r in log), default=0) + 1
    results = []
    for f in files:
        payload = f.path.read_bytes()
        sha = hashlib.sha256(payload).hexdigest()
        if (f.relative, sha) in seen_versions:
            continue  # this exact version was handled by an earlier run
        seen_versions.add((f.relative, sha))
        seq, next_seq = next_seq, next_seq + 1
        if sha in loaded_hashes:
            result = IngestResult(f, sha, seq, "duplicate_content", "", 0, 0, 0)
        elif f.arrival_date is None or f.business_date is None:
            result = IngestResult(
                f, sha, seq, "rejected", "", 0, 0, 0, "unrecognized landing path or file name"
            )
        else:
            try:
                text, encoding = _decode(payload)
                parse = _parse_csv if SOURCES[f.source] == "csv" else _parse_jsonl
                rows, repeats, errors = parse(text)
            except (UnicodeDecodeError, csv.Error) as exc:
                result = IngestResult(
                    f, sha, seq, "rejected", "", 0, 0, 0, f"{type(exc).__name__}: {exc}"[:300]
                )
            else:
                target = lake.bronze_file(f.source, f.arrival_date, Path(f.relative).stem, sha)
                atomic_write_table(_bronze_table(f, sha, seq, batch_id, rows), target)
                loaded_hashes.add(sha)
                result = IngestResult(f, sha, seq, "loaded", encoding, len(rows), repeats, errors)
        lake.write_ingest_log_entry(batch_id, result)
        results.append(result)
    return results
