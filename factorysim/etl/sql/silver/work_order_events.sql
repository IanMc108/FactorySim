-- CMMS work-order status events (JSON lines, ISO-8601 timestamps with offsets).
WITH parsed AS (
    SELECT
        *,
        NULLIF(trim(record ->> 'work_order'), '') AS work_order,
        upper(trim(record ->> 'event')) AS event,
        -- An explicit UTC offset is required: a bare local time is not guessed at.
        CASE WHEN regexp_full_match(record ->> 'timestamp',
                  '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)')
             THEN timezone('UTC', TRY_CAST(record ->> 'timestamp' AS TIMESTAMPTZ))
        END AS event_ts_utc,
        upper(trim(record ->> 'asset')) AS asset,
        money(record -> 'cost' ->> 'amount') AS cost,
        upper(record -> 'cost' ->> 'currency') AS currency
    FROM bronze
)
SELECT
    _source, _file, _record_number, _arrival_date, _file_business_date, _ingest_seq, _raw, record,
    CASE
        WHEN _parse_error IS NOT NULL THEN 'malformed_record'
        WHEN work_order IS NULL THEN 'missing_key'
        WHEN event IS NULL OR event NOT IN ('OPENED', 'COMPLETED') THEN 'invalid_event'
        WHEN event_ts_utc IS NULL THEN 'invalid_timestamp'
        WHEN event = 'COMPLETED'
             AND (cost IS NULL OR cost < 0 OR currency IS DISTINCT FROM 'USD')
            THEN 'invalid_amount'
    END AS reject_reason,
    CAST(timezone('America/Chicago', timezone('UTC', event_ts_utc)) AS DATE) AS business_date,
    work_order, event, event_ts_utc, asset, cost
FROM parsed
