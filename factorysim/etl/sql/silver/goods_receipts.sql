-- ERP goods receipts. Wafers are sometimes received in boxes of 25 (UoM BX25).
WITH parsed AS (
    SELECT
        *,
        NULLIF(trim(record ->> 'gr_number'), '') AS gr_no,
        NULLIF(trim(record ->> 'po_number'), '') AS po_no,
        CAST(strict_ts(record ->> 'receipt_date', '\d{4}-\d{2}-\d{2}', '%Y-%m-%d') AS DATE)
            AS receipt_date,
        upper(trim(record ->> 'material')) AS material_raw,
        strict_int(record ->> 'quantity') AS qty,
        upper(trim(record ->> 'uom')) AS uom,
        money(record ->> 'unit_cost')
            AS unit_cost,
        money(record ->> 'extended_cost')
            AS amount
    FROM bronze
)
SELECT
    _source, _file, _record_number, _arrival_date, _file_business_date, _arrival_ts, _ingest_seq,
    _raw, record,
    CASE
        WHEN _parse_error IS NOT NULL THEN 'malformed_record'
        WHEN gr_no IS NULL OR po_no IS NULL THEN 'missing_key'
        WHEN receipt_date IS NULL THEN 'invalid_date'
        WHEN qty IS NULL OR qty <= 0 THEN 'invalid_quantity'
        WHEN unit_cost IS NULL OR amount IS NULL OR unit_cost <= 0 THEN 'invalid_amount'
        WHEN abs(qty * unit_cost - amount) > 0.01 THEN 'amount_mismatch'
        WHEN m.material_code IS NULL THEN 'unknown_material'
        WHEN u.units_per_uom IS NULL THEN 'unknown_uom'
    END AS reject_reason,
    receipt_date AS business_date,
    gr_no, po_no, m.material_code,
    p.qty AS qty_received, p.uom AS uom_received,
    p.qty * u.units_per_uom AS qty_base,
    amount
FROM parsed p
LEFT JOIN ref_materials m ON m.material_code = p.material_raw
LEFT JOIN ref_uom_conversions u ON u.material_code = p.material_raw AND u.uom = p.uom
