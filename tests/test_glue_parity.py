"""The Glue (PySpark) port of the MES silver step must match the DuckDB pipeline exactly."""

import importlib.util
from pathlib import Path

import duckdb
import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from factorysim.config import SimConfig  # noqa: E402
from factorysim.db import create_database  # noqa: E402
from factorysim.etl.pipeline import run_pipeline  # noqa: E402
from factorysim.simulation import run_simulation  # noqa: E402
from factorysim.sources.export import export_sources  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
COLUMNS = [
    "_file",
    "_record_number",
    "business_date",
    "event_id",
    "revision",
    "event_ts_utc",
    "shift",
    "station_code",
    "sku",
    "lot_id",
    "qty",
    "operator_id",
    "comment",
    "business_month",
]


def _load_job():
    spec = importlib.util.spec_from_file_location(
        "glue_job", ROOT / "aws" / "glue" / "silver_production_events.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def job():
    return _load_job()


@pytest.fixture(scope="module")
def spark(job):
    builder = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
    )
    for key, value in job.SPARK_CONF.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    yield session
    session.stop()


def _compare(spark, job, lake: Path) -> int:
    reference = ROOT / "factorysim" / "etl" / "reference"
    stations = spark.read.option("header", True).csv(str(reference / "station_aliases.csv"))
    skus = spark.read.option("header", True).csv(str(reference / "sku_aliases.csv"))
    bronze = spark.read.parquet(str(lake / "bronze" / "mes_production"))
    log = spark.read.option("mergeSchema", True).parquet(
        str(lake / "_control" / "ingest_log" / "*" / "*.parquet")
    )
    silver, quarantine = job.transform(bronze, job.current_versions(log), stations, skus)

    spark_rows = sorted(tuple(r) for r in silver.select(*COLUMNS).collect())
    duck_rows = sorted(
        duckdb.connect()
        .execute(
            f"SELECT {', '.join(COLUMNS)} FROM read_parquet("
            f"'{lake}/silver/production_events/*/*.parquet', hive_partitioning = true, "
            "hive_types = {'business_month': VARCHAR})"
        )
        .fetchall()
    )
    assert spark_rows == duck_rows

    spark_q = sorted((r._file, r._record_number, r.reject_reason) for r in quarantine.collect())
    glob = lake / "quarantine" / "mes_production" / "*" / "*.parquet"
    duck_q = (
        sorted(
            duckdb.connect()
            .execute(f"SELECT _file, _record_number, reject_reason FROM read_parquet('{glob}')")
            .fetchall()
        )
        if list((lake / "quarantine").rglob("*.parquet"))
        else []
    )
    assert spark_q == duck_q
    return len(spark_rows)


def test_glue_transform_matches_duckdb_on_a_generated_year(spark, job, tmp_path):
    conn = create_database(tmp_path / "truth.db")
    run_simulation(conn, SimConfig(days=200))  # crosses the MES schema change on 2025-07-01
    export_sources(conn, tmp_path / "landing", tmp_path / "defects.jsonl")
    conn.close()
    run_pipeline(tmp_path / "landing", tmp_path / "lake")
    assert _compare(spark, job, tmp_path / "lake") > 5000


V1 = "event_id,rev,event_time,station,shift,lot,sku,qty,operator,comment\n"
V2 = (
    "event_id,revision,event_ts_utc,station_code,shift_code,lot_id,item_code,quantity,uom,"
    "operator_id,comment\n"
)
HOSTILE = {
    ("2025-03-09", "mes_production_20250308.csv"): V1
    + "H1,1,2025-03-09 03:30:00,lith-a ,C,L1,MPUXL-A,10,op1,\n"  # DST night, legacy codes
    + "H2,1,2025-03-08 13:00:00,LITHO-A,B,L1,MPU-XL-A,10.5,op1,\n"
    + "H3,1,2025-03-08 13:00:00,LITHO-A,B,L1,MPU-XL-A,1e3,op1,\n"
    + "H4,1,2025-03-08 13:00:00,LITHO-A,B,L1,MPU-XL-A,1_000,op1,\n"
    + "H5,1.0,2025-03-08 13:00:00,LITHO-A,B,L1,MPU-XL-A,5,op1,\n"
    + "H6,1,2025-03-08 13:00:00.250,LITHO-A,B,L1,MPU-XL-A,5,op1,\n"
    + "H7,1,2025-03-08 13:00:00,LITHO-A,B,L1,MPU-XL-A, 7 ,op1,\n"
    + "H8,1,2025-03-08 13:00:00,LITHO-A,B,L1,MPU-XL-A,-3,op1,\n"
    + "H9,1,2025-03-08 13:00:00,LITHO-Z,B,L1,MPU-XL-A,3,op1,\n"
    + "H10,1,2025-03-31 23:30:00,LITHO-B,C,L1,MPU-XL-B,4,op1,\n"
    + "H11,1,2025-03-08 13:00:00,LITHO-B,B\n",
    # H10 corrected into April (moves month); H1 re-sent as revision 2 in v2 format.
    ("2025-04-02", "mes_production_20250401.csv"): V2
    + "H10,2,2025-04-01T04:30:00Z,LITHO-B,1,L1,MPU-XL-B,4,EA,op1,\n"
    + "H1,2,2025-03-09T08:30:00Z,LITHO-A,3,L1,MPU-XL-A,12,EA,op1,\n"
    + "H12,1,2025-04-01T15:00:00Z,CHEM-A,2,L1,SUB-PE,9,BX,op1,\n",
}


def test_glue_transform_matches_duckdb_on_hostile_records(spark, job, tmp_path):
    for (arrival, name), body in HOSTILE.items():
        path = tmp_path / "landing" / "mes_production" / arrival.replace("-", "/") / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    run_pipeline(tmp_path / "landing", tmp_path / "lake")
    # Survivors: H1 (revision 2), H7 (" 7 "), H10 (revision 2, moved to April).
    assert _compare(spark, job, tmp_path / "lake") == 3


def test_glue_transform_accepts_an_empty_bronze_frame(spark, job, tmp_path):
    import pyarrow.parquet as pq

    from factorysim.etl.ingest import BRONZE_SCHEMA

    pq.write_table(BRONZE_SCHEMA.empty_table(), tmp_path / "empty.parquet")
    empty = spark.read.parquet(str(tmp_path / "empty.parquet"))
    reference = ROOT / "factorysim" / "etl" / "reference"
    stations = spark.read.option("header", True).csv(str(reference / "station_aliases.csv"))
    skus = spark.read.option("header", True).csv(str(reference / "sku_aliases.csv"))
    no_versions = empty.selectExpr(
        "_source AS source", "_file AS file_name", "_file_sha256 AS sha256"
    )
    silver, quarantine = job.transform(empty, no_versions, stations, skus)
    assert silver.count() == quarantine.count() == 0


def _fuzz_landing(root: Path, seed: int, n: int = 300) -> None:
    """Write MES files full of edge-case values, deterministically."""
    import csv
    import io
    import random

    rnd = random.Random(seed)
    ids = [f"F{i}" for i in range(n // 3)]  # shared ids exercise dedup and revisions
    stations = ["LITHO-A", "lith-a", " CHEM-B ", "EPC-A", "LITHO-Z", ""]
    skus = ["MPU-XL-A", "mpuxl-b", " SUB-PE", "MESH-AIN", "MPU-XL-SCRAP", ""]
    qtys = ["5", "0", "-3", "10.5", "1e3", " 7 ", "007", "", "1_000", "12", "10\n", "\t5", "5\r"]
    revs = ["1", "2", "3", "1.0", " 2", "x", "", "1\n"]

    def v1_time() -> str:
        day = rnd.choice(["2025-03-09", "2025-11-02", "2025-06-15"])
        h, m = rnd.randrange(0, 24), rnd.randrange(0, 60)
        good = f"{day} {h:02d}:{m:02d}:00"
        return rnd.choice([good] * 6 + [good + ".5", " " + good, good.replace("-0", "-", 1)])

    def v2_time() -> str:
        day = rnd.choice(["2025-11-02", "2025-07-04"])
        good = f"{day}T{rnd.randrange(24):02d}:{rnd.randrange(60):02d}:00Z"
        return rnd.choice([good] * 6 + [good.lower(), good[:-1] + "+00:00", good[:-1]])

    def write(arrival: str, name: str, header: list[str], rows: list[list[str]]) -> None:
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
        path = root / "landing" / "mes_production" / arrival.replace("-", "/") / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(buf.getvalue(), encoding="utf-8")

    v1 = [
        [
            rnd.choice(ids + ["", " F1 "]),
            rnd.choice(revs),
            v1_time(),
            rnd.choice(stations),
            rnd.choice("ABCabcD1"),
            "L1",
            rnd.choice(skus),
            rnd.choice(qtys),
            "op1",
            "",
        ]
        for _ in range(n // 2)
    ]
    v2 = [
        [
            rnd.choice(ids),
            rnd.choice(revs),
            v2_time(),
            rnd.choice(stations),
            rnd.choice(["1", "2", "3", "C", "4", ""]),
            "L1",
            rnd.choice(skus),
            rnd.choice(qtys),
            rnd.choice(["EA", "ea", "BX", ""]),
            "op1",
            "",
        ]
        for _ in range(n // 2)
    ]
    write(
        "2025-06-16",
        "mes_production_20250615.csv",
        [
            "event_id",
            "rev",
            "event_time",
            "station",
            "shift",
            "lot",
            "sku",
            "qty",
            "operator",
            "comment",
        ],
        v1,
    )
    write(
        "2025-11-03",
        "mes_production_20251102.csv",
        [
            "event_id",
            "revision",
            "event_ts_utc",
            "station_code",
            "shift_code",
            "lot_id",
            "item_code",
            "quantity",
            "uom",
            "operator_id",
            "comment",
        ],
        v2,
    )


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_glue_transform_matches_duckdb_on_fuzzed_records(spark, job, tmp_path, seed):
    _fuzz_landing(tmp_path, seed)
    run_pipeline(tmp_path / "landing", tmp_path / "lake")
    assert _compare(spark, job, tmp_path / "lake") > 0
