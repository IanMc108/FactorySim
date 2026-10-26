-- MES production events. Two schemas: v1 (to 2025-06-30) has local naive timestamps and
-- shifts A/B/C; v2 has UTC timestamps, shifts 1/2/3 and renamed columns. Timestamps must
-- match their pattern exactly; a v1 local time that falls in a DST gap or in the repeated
-- fall-back hour cannot be placed on the UTC timeline and is quarantined.
WITH parsed AS (
    SELECT
        *,
        NULLIF(trim(record ->> 'event_id'), '') AS event_id,
        strict_int(coalesce(record ->> 'revision', record ->> 'rev')) AS revision,
        strict_ts(record ->> 'event_ts_utc', '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',
                  '%Y-%m-%dT%H:%M:%SZ') AS v2_ts_utc,
        strict_ts(record ->> 'event_time', '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',
                  '%Y-%m-%d %H:%M:%S') AS v1_ts_local,
        upper(trim(coalesce(record ->> 'station_code', record ->> 'station'))) AS station_raw,
        CASE upper(trim(coalesce(record ->> 'shift_code', record ->> 'shift')))
            WHEN 'A' THEN 'A' WHEN '1' THEN 'A'
            WHEN 'B' THEN 'B' WHEN '2' THEN 'B'
            WHEN 'C' THEN 'C' WHEN '3' THEN 'C'
        END AS shift,
        coalesce(record ->> 'lot_id', record ->> 'lot') AS lot_id,
        upper(trim(coalesce(record ->> 'item_code', record ->> 'sku'))) AS sku_raw,
        strict_int(coalesce(record ->> 'quantity', record ->> 'qty')) AS qty,
        upper(coalesce(record ->> 'uom', 'EA')) AS uom,
        coalesce(record ->> 'operator_id', record ->> 'operator') AS operator_id,
        record ->> 'comment' AS comment
    FROM bronze
),
timed AS (
    SELECT
        *,
        CASE WHEN record ->> 'event_ts_utc' IS NOT NULL THEN v2_ts_utc
             ELSE local_to_utc(v1_ts_local) END AS event_ts_utc,
        CASE WHEN record ->> 'event_ts_utc' IS NULL THEN local_time_problem(v1_ts_local)
        END AS local_time_problem
    FROM parsed
),
resolved AS (
    SELECT
        p.*,
        s.station_code,
        k.sku,
        utc_to_local(p.event_ts_utc) AS event_ts_local
    FROM timed p
    LEFT JOIN ref_station_aliases s ON s.alias = p.station_raw
    LEFT JOIN ref_sku_aliases k ON k.alias = p.sku_raw
)
SELECT
    _source, _file, _record_number, _arrival_date, _file_business_date, _ingest_seq, _raw, record,
    CASE
        WHEN _parse_error IS NOT NULL THEN 'malformed_record'
        WHEN event_id IS NULL THEN 'missing_key'
        WHEN revision IS NULL THEN 'invalid_revision'
        WHEN event_ts_utc IS NULL THEN 'invalid_timestamp'
        WHEN local_time_problem IS NOT NULL THEN local_time_problem
        WHEN qty IS NULL THEN 'invalid_quantity'
        WHEN qty <= 0 THEN 'non_positive_quantity'
        WHEN shift IS NULL THEN 'invalid_shift'
        WHEN uom IS DISTINCT FROM 'EA' THEN 'unsupported_uom'
        WHEN station_code IS NULL THEN 'unknown_station'
        WHEN sku IS NULL THEN 'unknown_sku'
    END AS reject_reason,
    -- The night shift (C) runs 22:00-06:00; its post-midnight events belong to the
    -- production day the shift started on.
    CASE WHEN shift = 'C' AND hour(event_ts_local) < 6
         THEN CAST(event_ts_local AS DATE) - 1
         ELSE CAST(event_ts_local AS DATE)
    END AS business_date,
    event_id, revision, event_ts_utc, shift, station_code, sku, lot_id, qty, operator_id, comment
FROM resolved
