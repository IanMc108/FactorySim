-- ERP end-of-day stock snapshots (weekly).
WITH parsed AS (
    SELECT
        *,
        CAST(strict_ts(record ->> 'snapshot_date', '\d{4}-\d{2}-\d{2}', '%Y-%m-%d') AS DATE)
            AS snapshot_date,
        upper(trim(record ->> 'material')) AS material_raw,
        strict_int(record ->> 'on_hand') AS on_hand,
        upper(trim(record ->> 'uom')) AS uom
    FROM bronze
)
SELECT
    _source, _file, _record_number, _arrival_date, _file_business_date, _arrival_ts, _ingest_seq,
    _raw, record,
    CASE
        WHEN _parse_error IS NOT NULL THEN 'malformed_record'
        WHEN snapshot_date IS NULL THEN 'invalid_date'
        WHEN on_hand IS NULL OR on_hand < 0 THEN 'invalid_quantity'
        WHEN uom IS DISTINCT FROM 'EA' THEN 'unsupported_uom'
        WHEN m.material_code IS NULL THEN 'unknown_material'
    END AS reject_reason,
    snapshot_date AS business_date,
    snapshot_date, m.material_code, on_hand
FROM parsed p
LEFT JOIN ref_materials m ON m.material_code = p.material_raw
