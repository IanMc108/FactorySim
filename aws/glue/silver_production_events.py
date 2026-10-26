"""AWS Glue (5.0, Spark 3.5) port of the silver step for MES production events.

The local pipeline runs this step as DuckDB SQL (factorysim/etl/sql/silver/
production_events.sql). This is the same logic in PySpark, so the job can run on Glue
against the same lake layout on S3. ``tests/test_glue_parity.py`` runs ``transform()``
on local Spark over a real bronze table and asserts it produces exactly the rows the
DuckDB pipeline does.

Job flow on Glue:

1. Read only *new* bronze files through a job bookmark, to find which business months
   the batch touches.
2. Recompute those months from the full bronze table (deduplication needs every version
   of an event, not just the new ones).
3. Write silver and quarantine with dynamic partition overwrite, so untouched months
   are left alone.

Parameters: --JOB_NAME, --lake_path (e.g. s3://factorysim-lake), --reference_path.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

PLANT_TZ = "America/Chicago"


def _field(name: str):
    return F.get_json_object(F.col("record"), f"$.{name}")


def _upper_trim(col):
    return F.upper(F.trim(col))


def parse(bronze: DataFrame, station_aliases: DataFrame, sku_aliases: DataFrame) -> DataFrame:
    """Type, conform and validate bronze records. Requires spark.sql.session.timeZone=UTC."""
    shift_raw = _upper_trim(F.coalesce(_field("shift_code"), _field("shift")))
    parsed = bronze.select(
        "_source",
        "_file",
        "_record_number",
        "_arrival_date",
        "_file_business_date",
        "_parse_error",
        "_raw",
        "record",
        F.nullif(F.trim(_field("event_id")), F.lit("")).alias("event_id"),
        F.coalesce(_field("revision"), _field("rev")).cast("int").alias("revision"),
        F.when(
            _field("event_ts_utc").isNotNull(),
            F.to_timestamp(_field("event_ts_utc"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .otherwise(
            F.to_utc_timestamp(
                F.to_timestamp(_field("event_time"), "yyyy-MM-dd HH:mm:ss"), PLANT_TZ
            )
        )
        .alias("event_ts_utc"),
        _upper_trim(F.coalesce(_field("station_code"), _field("station"))).alias("station_raw"),
        F.when(shift_raw.isin("A", "1"), "A")
        .when(shift_raw.isin("B", "2"), "B")
        .when(shift_raw.isin("C", "3"), "C")
        .alias("shift"),
        F.coalesce(_field("lot_id"), _field("lot")).alias("lot_id"),
        _upper_trim(F.coalesce(_field("item_code"), _field("sku"))).alias("sku_raw"),
        F.coalesce(_field("quantity"), _field("qty")).cast("int").alias("qty"),
        F.upper(F.coalesce(_field("uom"), F.lit("EA"))).alias("uom"),
        F.coalesce(_field("operator_id"), _field("operator")).alias("operator_id"),
        _field("comment").alias("comment"),
    )
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
        .when(F.col("qty").isNull(), "invalid_quantity")
        .when(F.col("qty") <= 0, "non_positive_quantity")
        .when(F.col("shift").isNull(), "invalid_shift")
        .when(F.col("uom") != "EA", "unsupported_uom")
        .when(F.col("station_code").isNull(), "unknown_station")
        .when(F.col("sku").isNull(), "unknown_sku")
    )
    local_date = F.to_date("event_ts_local")
    business_date = F.when(
        (F.col("shift") == "C") & (F.hour("event_ts_local") < 6), F.date_sub(local_date, 1)
    ).otherwise(local_date)
    return resolved.withColumn("reject_reason", reject).withColumn("business_date", business_date)


def transform(
    bronze: DataFrame, station_aliases: DataFrame, sku_aliases: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Return (silver, quarantine) for MES production events."""
    validated = parse(bronze, station_aliases, sku_aliases)
    latest_first = Window.partitionBy("event_id").orderBy(
        F.col("revision").desc(),
        F.col("_arrival_date").desc(),
        F.col("_file").desc(),
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
        "record",
        F.date_format("_file_business_date", "yyyy-MM").alias("business_month"),
    )
    return silver, quarantine


def main() -> None:  # pragma: no cover - runs on AWS Glue only
    import sys

    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "lake_path", "reference_path"])
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    lake, ref = args["lake_path"].rstrip("/"), args["reference_path"].rstrip("/")
    stations = spark.read.option("header", True).csv(f"{ref}/station_aliases.csv")
    skus = spark.read.option("header", True).csv(f"{ref}/sku_aliases.csv")

    # Only new bronze files come through the bookmark; they decide which months to rebuild.
    new_bronze = glue.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [f"{lake}/bronze/mes_production/"], "recurse": True},
        format="parquet",
        transformation_ctx="bronze_mes_production",
    ).toDF()
    touched = [
        r.month
        for r in parse(new_bronze, stations, skus)
        .select(
            F.date_format(F.coalesce("business_date", "_file_business_date"), "yyyy-MM").alias(
                "month"
            )
        )
        .distinct()
        .collect()
    ]
    if touched:
        bronze = spark.read.parquet(f"{lake}/bronze/mes_production/")
        silver, quarantine = transform(bronze, stations, skus)
        (
            silver.filter(F.col("business_month").isin(touched))
            .write.mode("overwrite")
            .partitionBy("business_month")
            .parquet(f"{lake}/silver/production_events/")
        )
        (
            quarantine.filter(F.col("business_month").isin(touched))
            .write.mode("overwrite")
            .partitionBy("business_month")
            .parquet(f"{lake}/quarantine/mes_production/")
        )
    job.commit()


if __name__ == "__main__":  # pragma: no cover
    main()
