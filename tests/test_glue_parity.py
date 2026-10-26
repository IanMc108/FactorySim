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
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_glue_transform_matches_duckdb(spark, tmp_path):
    conn = create_database(tmp_path / "truth.db")
    run_simulation(conn, SimConfig(days=200))  # crosses the MES schema change on 2025-07-01
    export_sources(conn, tmp_path / "landing", tmp_path / "defects.jsonl")
    conn.close()
    lake = tmp_path / "lake"
    run_pipeline(tmp_path / "landing", lake)

    job = _load_job()
    reference = ROOT / "factorysim" / "etl" / "reference"
    stations = spark.read.option("header", True).csv(str(reference / "station_aliases.csv"))
    skus = spark.read.option("header", True).csv(str(reference / "sku_aliases.csv"))
    bronze = spark.read.parquet(str(lake / "bronze" / "mes_production"))
    silver, quarantine = job.transform(bronze, stations, skus)

    spark_rows = sorted(tuple(r) for r in silver.select(*COLUMNS).collect())
    duck_rows = sorted(
        duckdb.connect()
        .execute(
            f"SELECT {', '.join(COLUMNS)} FROM "
            f"read_parquet('{lake}/silver/production_events/*/*.parquet')"
        )
        .fetchall()
    )
    assert len(spark_rows) == len(duck_rows) > 5000
    assert spark_rows == duck_rows

    spark_q = sorted((r._file, r._record_number, r.reject_reason) for r in quarantine.collect())
    duck_q = sorted(
        duckdb.connect()
        .execute(
            f"SELECT _file, _record_number, reject_reason FROM "
            f"read_parquet('{lake}/quarantine/mes_production/*/*.parquet')"
        )
        .fetchall()
    )
    assert spark_q == duck_q
