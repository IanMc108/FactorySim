-- MES material issues: material consumed by a station at the start of each shift.
WITH parsed AS (
    SELECT
        *,
        NULLIF(trim(record ->> 'issue_id'), '') AS issue_id,
        strict_ts(record ->> 'issue_time_local', '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',
                  '%Y-%m-%d %H:%M:%S') AS issue_ts_local,
        upper(trim(record ->> 'station')) AS station_raw,
        upper(trim(record ->> 'shift')) AS shift,
        upper(trim(record ->> 'material')) AS material_raw,
        strict_int(record ->> 'qty') AS qty,
        upper(coalesce(record ->> 'uom', 'EA')) AS uom
    FROM bronze
)
SELECT
    _source, _file, _record_number, _arrival_date, _file_business_date, _arrival_ts, _ingest_seq,
    _raw, record,
    CASE
        WHEN _parse_error IS NOT NULL THEN 'malformed_record'
        WHEN issue_id IS NULL THEN 'missing_key'
        WHEN issue_ts_local IS NULL THEN 'invalid_timestamp'
        WHEN local_time_problem(issue_ts_local) IS NOT NULL
            THEN local_time_problem(issue_ts_local)
        WHEN qty IS NULL THEN 'invalid_quantity'
        WHEN qty <= 0 THEN 'non_positive_quantity'
        WHEN shift IS NULL OR shift NOT IN ('A', 'B', 'C') THEN 'invalid_shift'
        WHEN uom IS DISTINCT FROM 'EA' THEN 'unsupported_uom'
        WHEN s.station_code IS NULL THEN 'unknown_station'
        WHEN m.material_code IS NULL THEN 'unknown_material'
    END AS reject_reason,
    CASE WHEN shift = 'C' AND hour(issue_ts_local) < 6
         THEN CAST(issue_ts_local AS DATE) - 1
         ELSE CAST(issue_ts_local AS DATE)
    END AS business_date,
    issue_id,
    local_to_utc(issue_ts_local) AS issue_ts_utc,
    shift, s.station_code, m.material_code, qty
FROM parsed p
LEFT JOIN ref_station_aliases s ON s.alias = p.station_raw
LEFT JOIN ref_materials m ON m.material_code = p.material_raw
