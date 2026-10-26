-- MES production events. Two schemas: v1 (to 2025-06-30) has local naive timestamps and
-- shifts A/B/C; v2 has UTC timestamps, shifts 1/2/3 and renamed columns.
WITH parsed AS (
    SELECT
        *,
        NULLIF(trim(record ->> 'event_id'), '') AS event_id,
        strict_int(coalesce(record ->> 'revision', record ->> 'rev')) AS revision,
        CASE
            WHEN record ->> 'event_ts_utc' IS NOT NULL
                THEN try_strptime(record ->> 'event_ts_utc', '%Y-%m-%dT%H:%M:%SZ')
            ELSE timezone('UTC', timezone('America/Chicago',
                     try_strptime(record ->> 'event_time', '%Y-%m-%d %H:%M:%S')))
        END AS event_ts_utc,
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
resolved AS (
    SELECT
        p.*,
        s.station_code,
        k.sku,
        timezone('America/Chicago', timezone('UTC', p.event_ts_utc)) AS event_ts_local
    FROM parsed p
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
        WHEN qty IS NULL THEN 'invalid_quantity'
        WHEN qty <= 0 THEN 'non_positive_quantity'
        WHEN shift IS NULL THEN 'invalid_shift'
        WHEN uom <> 'EA' THEN 'unsupported_uom'
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
