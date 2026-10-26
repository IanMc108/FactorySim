"""AWS Glue (5.0, Spark 3.5) port of the silver step for MES production events.

The local pipeline runs this step as DuckDB SQL (factorysim/etl/sql/silver/
production_events.sql plus the latest-wins and partition-sync logic in pipeline.py). This
is the same logic in PySpark, so the job can run on Glue against the same lake layout on
S3. ``tests/test_glue_parity.py`` runs ``transform()`` on local Spark, over a generated
year and over hand-made hostile records, and asserts it produces exactly the rows the
DuckDB pipeline does.

Job flow on Glue (``main``, which has not been run on AWS):

1. A job bookmark over bronze tells the job whether any new file arrived; with none, the
   job commits and exits.
2. Silver is recomputed from all of bronze (latest-wins needs every version of a key,
   and a correction can move a record to another month).
3. Each month is fingerprinted on both sides; only months whose content changed are
   overwritten (dynamic partition overwrite) and months left empty are deleted.

Parameters: --JOB_NAME, --lake_path (e.g. s3://factorysim-lake), --reference_path.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

PLANT_TZ = "America/Chicago"
SPARK_CONF = {
    "spark.sql.session.timeZone": "UTC",
    # Unparseable timestamps become NULL (and are quarantined) instead of failing the job.
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
}


def _field(name: str):
    return F.get_json_object(F.col("record"), f"$.{name}")


def _upper_trim(col):
    return F.upper(F.trim(col))


def _strict_ts(col, pattern: str, fmt: str):
    """Parse only strings that match the pattern exactly, as the DuckDB strict_ts macro does."""
    return F.when(col.rlike(pattern), F.to_timestamp(col, fmt))


def _strict_int(col):
    """Plain digits only: "10.5", "1e3" and "1_000" are rejected, never rounded."""
    trimmed = F.trim(col)
    return F.when(trimmed.rlike(r"^-?[0-9]+$"), trimmed.cast("bigint"))


def current_versions(ingest_log: DataFrame) -> DataFrame:
    """(source, file_name, sha256) of each file name's current version, from the ingest log.

    The current version is the loaded or duplicate entry with the latest arrival date, then
    the latest ingest sequence, exactly as in factorysim/etl/lake.py.
    """
    file_name = F.regexp_extract("file", r"[^/]+$", 0)
    latest = Window.partitionBy("source", file_name).orderBy(
        F.col("arrival_date").desc(), F.col("seq").desc()
    )
    return (
        ingest_log.filter(F.col("status").isin("loaded", "duplicate_content"))
        .withColumn("_rank", F.row_number().over(latest))
        .filter("_rank = 1")
        .select("source", file_name.alias("file_name"), "sha256")
    )


def current_records(bronze: DataFrame, versions: DataFrame) -> DataFrame:
    """Keep only bronze records that belong to their file name's current version."""
    keyed = bronze.withColumn("_file_name", F.regexp_extract("_file", r"[^/]+$", 0))
    match = (
        (keyed._source == versions.source)
        & (keyed._file_name == versions.file_name)
        & (keyed._file_sha256 == versions.sha256)
    )
    return keyed.join(F.broadcast(versions), match, "left_semi").drop("_file_name")


def parse(bronze: DataFrame, station_aliases: DataFrame, sku_aliases: DataFrame) -> DataFrame:
    """Type, conform and validate bronze records. Needs the session settings in SPARK_CONF."""
    shift_raw = _upper_trim(F.coalesce(_field("shift_code"), _field("shift")))
    parsed = bronze.select(
        "_source",
        "_file",
        "_record_number",
        "_arrival_date",
        "_file_business_date",
        "_ingest_seq",
        "_parse_error",
        "_raw",
        "record",
        F.nullif(F.trim(_field("event_id")), F.lit("")).alias("event_id"),
        _strict_int(F.coalesce(_field("revision"), _field("rev"))).alias("revision"),
        _field("event_ts_utc").isNotNull().alias("_is_v2"),
        _strict_ts(
            _field("event_ts_utc"),
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
        ).alias("_v2_ts_utc"),
        _strict_ts(
            _field("event_time"), r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", "yyyy-MM-dd HH:mm:ss"
        ).alias("_v1_ts_local"),
        _upper_trim(F.coalesce(_field("station_code"), _field("station"))).alias("station_raw"),
        F.when(shift_raw.isin("A", "1"), "A")
        .when(shift_raw.isin("B", "2"), "B")
        .when(shift_raw.isin("C", "3"), "C")
        .alias("shift"),
        F.coalesce(_field("lot_id"), _field("lot")).alias("lot_id"),
        _upper_trim(F.coalesce(_field("item_code"), _field("sku"))).alias("sku_raw"),
        _strict_int(F.coalesce(_field("quantity"), _field("qty"))).alias("qty"),
        F.upper(F.coalesce(_field("uom"), F.lit("EA"))).alias("uom"),
        F.coalesce(_field("operator_id"), _field("operator")).alias("operator_id"),
        _field("comment").alias("comment"),
    )
    utc = F.to_utc_timestamp("_v1_ts_local", PLANT_TZ)

    def local(ts):
        return F.from_utc_timestamp(ts, PLANT_TZ)

    one_hour = F.expr("INTERVAL 1 HOUR")
    local_problem = (
        F.when(F.col("_is_v2") | F.col("_v1_ts_local").isNull(), F.lit(None))
        .when(local(utc) != F.col("_v1_ts_local"), "nonexistent_local_time")
        .when(
            (local(utc + one_hour) == F.col("_v1_ts_local"))
            | (local(utc - one_hour) == F.col("_v1_ts_local")),
            "ambiguous_local_time",
        )
    )
    parsed = parsed.withColumn(
        "event_ts_utc", F.when(F.col("_is_v2"), F.col("_v2_ts_utc")).otherwise(utc)
    ).withColumn("_local_time_problem", local_problem)
    resolved = (
        parsed.join(
            F.broadcast(station_aliases.withColumnRenamed("alias", "station_raw")),
            "station_raw",
            "left",
        )
        .join(F.broadcast(sku_aliases.withColumnRenamed("alias", "sku_raw")), "sku_raw", "left")
        .withColumn("event_ts_local", F.from_utc_timestamp("event_ts_utc", PLANT_TZ))
    )
    reject = (
        F.when(F.col("_parse_error").isNotNull(), "malformed_record")
        .when(F.col("event_id").isNull(), "missing_key")
        .when(F.col("revision").isNull(), "invalid_revision")
        .when(F.col("event_ts_utc").isNull(), "invalid_timestamp")
        .when(F.col("_local_time_problem").isNotNull(), F.col("_local_time_problem"))
        .when(F.col("qty").isNull(), "invalid_quantity")
        .when(F.col("qty") <= 0, "non_positive_quantity")
        .when(F.col("shift").isNull(), "invalid_shift")
        .when(~F.col("uom").eqNullSafe("EA"), "unsupported_uom")
        .when(F.col("station_code").isNull(), "unknown_station")
        .when(F.col("sku").isNull(), "unknown_sku")
    )
    local_date = F.to_date("event_ts_local")
    business_date = F.when(
        (F.col("shift") == "C") & (F.hour("event_ts_local") < 6), F.date_sub(local_date, 1)
    ).otherwise(local_date)
    return resolved.withColumn("reject_reason", reject).withColumn("business_date", business_date)


def transform(
    bronze: DataFrame, versions: DataFrame, station_aliases: DataFrame, sku_aliases: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Return (silver, quarantine) for MES production events."""
    validated = parse(current_records(bronze, versions), station_aliases, sku_aliases)
    latest_first = Window.partitionBy("event_id").orderBy(
        F.col("revision").desc(),
        F.col("_file_business_date").desc(),
        F.col("_ingest_seq").desc(),
        F.col("_record_number").desc(),
    )
    silver = (
        validated.filter(F.col("reject_reason").isNull())
        .withColumn("_rank", F.row_number().over(latest_first))
        .filter("_rank = 1")
        .select(
            "_source",
            "_file",
            "_record_number",
            "_arrival_date",
            "_file_business_date",
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
            F.date_format("business_date", "yyyy-MM").alias("business_month"),
        )
    )
    quarantine = validated.filter(F.col("reject_reason").isNotNull()).select(
        "_source",
        "_file",
        "_record_number",
        "_arrival_date",
        "_file_business_date",
        "reject_reason",
        "_raw",
        F.col("record").cast("string").alias("record"),
        F.date_format("_file_business_date", "yyyy-MM").alias("business_month"),
    )
    return silver, quarantine


def month_fingerprints(df: DataFrame) -> dict[str, tuple[int, int]]:
    """Row count and sum of row hashes per business_month."""
    columns = [c for c in df.columns if c != "business_month"]
    rows = (
        df.groupBy("business_month")
        .agg(F.count("*").alias("n"), F.sum(F.xxhash64(*columns).cast("decimal(38,0)")).alias("h"))
        .collect()
    )
    return {r.business_month: (r.n, r.h) for r in rows}


def sync_partitions(spark, new: DataFrame, path: str) -> list[str]:  # pragma: no cover
    """Overwrite only changed months under ``path`` and delete months left empty."""
    jvm = spark.sparkContext._jvm
    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    old = month_fingerprints(spark.read.parquet(path)) if fs.exists(fs_path) else {}
    fresh = month_fingerprints(new)
    changed = sorted(m for m in fresh if old.get(m) != fresh[m])
    if changed:
        (
            new.filter(F.col("business_month").isin(changed))
            .write.mode("overwrite")
            .partitionBy("business_month")
            .parquet(path)
        )
    for month in sorted(set(old) - set(fresh)):
        fs.delete(jvm.org.apache.hadoop.fs.Path(f"{path}/business_month={month}"), True)
    return changed


def main() -> None:  # pragma: no cover - runs on AWS Glue only
    import sys

    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "lake_path", "reference_path"])
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    for key, value in SPARK_CONF.items():
        spark.conf.set(key, value)
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    lake, ref = args["lake_path"].rstrip("/"), args["reference_path"].rstrip("/")
    new_bronze = glue.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [f"{lake}/bronze/mes_production/"], "recurse": True},
        format="parquet",
        transformation_ctx="bronze_mes_production",
    ).toDF()
    # With no new files the bookmark yields an empty, possibly schemaless frame.
    if new_bronze.columns and not new_bronze.isEmpty():
        stations = spark.read.option("header", True).csv(f"{ref}/station_aliases.csv")
        skus = spark.read.option("header", True).csv(f"{ref}/sku_aliases.csv")
        bronze = spark.read.parquet(f"{lake}/bronze/mes_production/")
        log = spark.read.option("mergeSchema", True).parquet(
            f"{lake}/_control/ingest_log/*/*.parquet"
        )
        silver, quarantine = transform(bronze, current_versions(log), stations, skus)
        sync_partitions(spark, silver, f"{lake}/silver/production_events")
        sync_partitions(spark, quarantine, f"{lake}/quarantine/mes_production")
    job.commit()


if __name__ == "__main__":  # pragma: no cover
    main()
