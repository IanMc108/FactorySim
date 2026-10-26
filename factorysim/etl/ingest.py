"""Landing -> bronze: load each new file version as-is, with lineage, exactly once.

Bronze keeps every record of every file version the pipeline has accepted, untyped, so
any later layer can be rebuilt from it. Each record carries its source file, the file's
SHA-256, the arrival and business dates, the file's ingest sequence number and its record
number in the file. Records that cannot be split into the header's columns are kept with a
``_parse_error`` (and their raw text) rather than dropped.

Every arrival of a file is an event in the ingest log:

* a path whose size, modification and change times match its last log entry is skipped
  without being read (a re-run);
* a file with a bad name or folder, or an arrival date before its business date or more
  than 60 days after it, is logged as ``rejected`` whatever its content;
* a path whose content hash matches its last entry is logged as ``touched``;
* content already loaded under the same file name (a re-delivery, or a revert to an
  earlier version) is logged as ``duplicate_content`` and not stored again;
* new content is decoded, split into records and written to bronze as ``loaded``;
* a file that cannot be read (undecodable bytes, an empty CSV, no header) is logged as
  ``rejected`` and the rest of the batch carries on. Marker files such as ``_SUCCESS``
  are ignored.

Which version of a file name is *current* is decided from the log, not from bronze: the
loaded or duplicate entry with the latest arrival date, then the latest ingest sequence.
So a revert, a header-only re-issue that withdraws every record, and a version that lands
in an earlier arrival folder are all handled the same way in incremental and full runs.

Each file is committed on its own: its bronze Parquet is written to a temporary name and
renamed, then its ingest-log entry is written the same way. Bronze is never deleted. Every
bronze file carries its ingest metadata, so a crash between the two writes (or a lost log)
is repaired by rebuilding the log entry from the bronze file; a logged bronze file that has
gone missing is rebuilt from the landing zone, or reported if the landing file is gone too.
At the end of a batch its per-file log entries are compacted into one file.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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
        ("_arrival_ts", pa.timestamp("us")),
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
    "_file_business_date DATE, _arrival_ts TIMESTAMP, _ingest_seq BIGINT, "
    "_record_number INTEGER, "
    "_batch_id VARCHAR, _parse_error VARCHAR, _raw VARCHAR, record VARCHAR"
)


@dataclass(frozen=True)
class LandedFile:
    source: str
    path: Path
    relative: str
    arrival_date: date | None
    business_date: date | None
    size: int = 0
    mtime_ns: int = 0
    ctime_ns: int = 0

    @property
    def name(self) -> str:
        return Path(self.relative).name

    @property
    def arrival_ts(self) -> datetime:
        """When the file landed: its modification time (on S3, LastModified)."""
        return datetime.fromtimestamp(self.mtime_ns / 1e9, timezone.utc).replace(tzinfo=None)


@dataclass(frozen=True)
class IngestResult:
    file: LandedFile
    sha256: str
    seq: int
    status: str  # loaded | duplicate_content | touched | rejected
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
            stat = path.stat()
            found.append(
                LandedFile(
                    source,
                    path,
                    relative,
                    arrival,
                    business,
                    stat.st_size,
                    stat.st_mtime_ns,
                    stat.st_ctime_ns,
                )
            )
    # Arrival order: the arrival folder, then the time the file actually landed, so a file
    # that lands after an earlier run sorts after everything that run saw.
    return sorted(
        found, key=lambda f: (f.arrival_date or date.min, f.mtime_ns, f.source, f.relative)
    )


def _decode(payload: bytes) -> tuple[str, str]:
    try:
        return payload.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        return payload.decode("cp1252"), "cp1252"  # raises if not valid cp1252 either


MAX_ARRIVAL_DELAY_DAYS = 60


class UnreadableFile(ValueError):
    """The file is not a usable export (empty, no header, ...)."""


def _parse_csv(text: str) -> tuple[list[tuple], int, int]:
    if not text.strip():
        raise UnreadableFile("empty file")
    reader = csv.reader(io.StringIO(text, newline=""))
    header = None
    rows, repeats, errors = [], 0, 0
    for number, fields in enumerate(reader, start=1):
        if header is None:
            header = [h.strip() for h in fields]
            if not header or not all(header):
                raise UnreadableFile("missing or blank header")
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
    # JSON lines have no header, so an empty file is a valid "no records" extract (a quiet
    # day, or a re-issue that withdraws a day); file_version_shrink raises the latter.
    rows, errors = [], 0
    # Split on "\n" only: str.splitlines() would also break on U+2028 and friends, which
    # are legal inside JSON strings.
    for number, line in enumerate(text.split("\n"), start=1):
        line = line.removesuffix("\r")
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
            "_arrival_ts": pa.array([f.arrival_ts] * n, pa.timestamp("us")),
            "_ingest_seq": pa.array([seq] * n, pa.int64()),
            "_record_number": pa.array(columns[0], pa.int32()),
            "_batch_id": pa.array([batch_id] * n, pa.string()),
            "_parse_error": pa.array(columns[1], pa.string()),
            "_raw": pa.array(columns[2], pa.string()),
            "record": pa.array(columns[3], pa.string()),
        },
        schema=BRONZE_SCHEMA,
    )


def _with_file_metadata(table: pa.Table, result: IngestResult, batch_id: str) -> pa.Table:
    """Make the bronze file self-describing, so the ingest log can be rebuilt from it."""
    f = result.file
    meta = {
        "source": f.source,
        "file": f.relative,
        "sha256": result.sha256,
        "arrival_date": str(f.arrival_date),
        "file_business_date": str(f.business_date),
        "arrival_ts": f.arrival_ts.isoformat(),
        "seq": result.seq,
        "batch_id": batch_id,
        "size": f.size,
        "mtime_ns": f.mtime_ns,
        "ctime_ns": f.ctime_ns,
        "encoding": result.encoding,
        "records": result.records,
        "header_repeats": result.header_repeats,
        "parse_errors": result.parse_errors,
    }
    return table.replace_schema_metadata({"factorysim.ingest": json.dumps(meta)})


def ingest(
    conn: duckdb.DuckDBPyConnection, lake: Lake, files: list[LandedFile], batch_id: str
) -> list[IngestResult]:
    """Load new file versions into bronze, committing each file and its log entry in turn."""
    existing_bronze = lake.recover_orphan_bronze(conn)
    log = lake.ingest_log(conn)

    def bronze_path(r: dict) -> Path:
        return lake.bronze_file(r["source"], r["arrival_date"], Path(r["file"]).stem, r["sha256"])

    # A loaded version whose bronze file has gone missing is treated as never loaded, so
    # the landing file is read and ingested again.
    lost = {
        r["sha256"]
        for r in log
        if r["status"] == "loaded" and bronze_path(r) not in existing_bronze
    }
    loaded = {
        (r["source"], Path(r["file"]).name, r["sha256"])
        for r in log
        if r["status"] == "loaded" and r["sha256"] not in lost
    }
    last_by_path = {r["file"]: r for r in log}  # log is ordered by seq
    next_seq = max((r["seq"] for r in log), default=0) + 1
    results = []
    for f in files:
        last = last_by_path.get(f.relative)
        stamp = (f.size, f.mtime_ns, f.ctime_ns)
        if (
            last
            and last["sha256"] not in lost
            and (last.get("size"), last.get("mtime_ns"), last.get("ctime_ns")) == stamp
        ):
            continue  # unchanged since it was last seen (size, mtime and ctime): not read
        payload = f.path.read_bytes()
        sha = hashlib.sha256(payload).hexdigest()
        seq, next_seq = next_seq, next_seq + 1
        # The path and date rules come first, so a copy of good content in a bad place is
        # rejected like anything else there (it must not count as a duplicate or a touch).
        if f.arrival_date is None or f.business_date is None:
            problem = "unrecognized landing path or file name"
        elif not (
            f.business_date
            <= f.arrival_date
            <= f.business_date + timedelta(days=MAX_ARRIVAL_DELAY_DAYS)
        ):
            problem = (
                f"implausible arrival date {f.arrival_date} for business date {f.business_date}"
            )
        else:
            problem = None
        if problem:
            result = IngestResult(f, sha, seq, "rejected", "", 0, 0, 0, problem)
        elif last and last["sha256"] == sha and sha not in lost:
            # Same content, new stamp: record it so the next run skips the read.
            result = IngestResult(f, sha, seq, "touched", "", 0, 0, 0)
        elif (f.source, f.name, sha) in loaded:
            result = IngestResult(f, sha, seq, "duplicate_content", "", 0, 0, 0)
        else:
            try:
                text, encoding = _decode(payload)
                parse = _parse_csv if SOURCES[f.source] == "csv" else _parse_jsonl
                rows, repeats, errors = parse(text)
            except (UnicodeDecodeError, csv.Error, UnreadableFile) as exc:
                result = IngestResult(
                    f, sha, seq, "rejected", "", 0, 0, 0, f"{type(exc).__name__}: {exc}"[:300]
                )
            else:
                result = IngestResult(f, sha, seq, "loaded", encoding, len(rows), repeats, errors)
                target = lake.bronze_file(f.source, f.arrival_date, Path(f.relative).stem, sha)
                table = _bronze_table(f, sha, seq, batch_id, rows)
                atomic_write_table(_with_file_metadata(table, result, batch_id), target)
                loaded.add((f.source, f.name, sha))
                lost.discard(sha)
        lake.write_ingest_log_entry(batch_id, result)
        last_by_path[f.relative] = {
            "sha256": sha,
            "size": f.size,
            "mtime_ns": f.mtime_ns,
            "ctime_ns": f.ctime_ns,
        }
        results.append(result)
    lake.compact_ingest_log(batch_id)
    return results
