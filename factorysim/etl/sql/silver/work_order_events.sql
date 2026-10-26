-- CMMS work-order status events (JSON lines, ISO-8601 timestamps with offsets).
WITH parsed AS (
    SELECT
        *,
        NULLIF(trim(record ->> 'work_order'), '') AS work_order,
        upper(trim(record ->> 'event')) AS event,
        timezone('UTC', TRY_CAST(record ->> 'timestamp' AS TIMESTAMPTZ)) AS event_ts_utc,
        upper(trim(record ->> 'asset')) AS asset,
        TRY_CAST(record -> 'cost' ->> 'amount' AS DECIMAL(18, 2)) AS cost,
        upper(record -> 'cost' ->> 'currency') AS currency
    FROM bronze
)
SELECT
    _source, _file, _record_number, _arrival_date, _file_business_date, _raw, record,
    CASE
        WHEN _parse_error IS NOT NULL THEN 'malformed_record'
        WHEN work_order IS NULL THEN 'missing_key'
        WHEN event NOT IN ('OPENED', 'COMPLETED') THEN 'invalid_event'
        WHEN event_ts_utc IS NULL THEN 'invalid_timestamp'
        WHEN event = 'COMPLETED' AND (cost IS NULL OR currency <> 'USD') THEN 'invalid_amount'
    END AS reject_reason,
    CAST(timezone('America/Chicago', timezone('UTC', event_ts_utc)) AS DATE) AS business_date,
    work_order, event, event_ts_utc, asset, cost
FROM parsed
