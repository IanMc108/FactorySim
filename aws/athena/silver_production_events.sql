-- Athena table over the silver MES production events written by the pipeline (or by
-- aws/glue/silver_production_events.py). Not deployed; replace the bucket name.
--
-- Partition projection derives partitions from the business_month=YYYY-MM prefixes, so
-- months that partition sync creates or deletes appear and disappear with no MSCK REPAIR,
-- crawler or Glue partition API calls. The partition column lives only in the path.
CREATE EXTERNAL TABLE IF NOT EXISTS factorysim_silver.production_events (
    _source              string,
    _file                string,
    _record_number       int,
    _arrival_date        date,
    _file_business_date  date,
    business_date        date,
    event_id             string,
    revision             bigint,
    event_ts_utc         timestamp,
    shift                string,
    station_code         string,
    sku                  string,
    lot_id               string,
    qty                  bigint,
    operator_id          string,
    comment              string
)
PARTITIONED BY (business_month string)
STORED AS PARQUET
LOCATION 's3://factorysim-lake/silver/production_events/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.business_month.type' = 'date',
    'projection.business_month.format' = 'yyyy-MM',
    'projection.business_month.interval' = '1',
    'projection.business_month.interval.unit' = 'MONTHS',
    'projection.business_month.range' = '2024-01,NOW',
    'storage.location.template' =
        's3://factorysim-lake/silver/production_events/business_month=${business_month}/'
);
