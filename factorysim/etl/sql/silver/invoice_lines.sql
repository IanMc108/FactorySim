-- ERP invoice and credit-memo lines. Money arrives as text in several formats
-- ("$1,000.00", "1,000.00", "1000"), dates as MM/DD/YYYY or ISO.
WITH parsed AS (
    SELECT
        *,
        NULLIF(trim(record ->> 'document_no'), '') AS doc_no,
        upper(trim(record ->> 'doc_type')) AS doc_type,
        NULLIF(trim(record ->> 'reference_doc'), '') AS reference_doc,
        CAST(coalesce(strict_ts(record ->> 'document_date', '\d{2}/\d{2}/\d{4}', '%m/%d/%Y'),
                      strict_ts(record ->> 'document_date', '\d{4}-\d{2}-\d{2}', '%Y-%m-%d'))
             AS DATE) AS doc_date,
        trim(record ->> 'customer_id') AS customer_id,
        strict_int(record ->> 'line_no') AS line_no,
        upper(trim(record ->> 'sku')) AS sku_raw,
        strict_int(record ->> 'quantity') AS qty,
        money(record ->> 'unit_price')
            AS unit_price,
        money(record ->> 'line_amount')
            AS line_amount,
        upper(trim(record ->> 'currency')) AS currency
    FROM bronze
)
SELECT
    _source, _file, _record_number, _arrival_date, _file_business_date, _ingest_seq, _raw, record,
    CASE
        WHEN _parse_error IS NOT NULL THEN 'malformed_record'
        WHEN doc_no IS NULL OR line_no IS NULL THEN 'missing_key'
        WHEN doc_type IS NULL OR doc_type NOT IN ('INV', 'CM') THEN 'invalid_doc_type'
        WHEN doc_date IS NULL THEN 'invalid_date'
        WHEN qty IS NULL OR qty = 0 THEN 'invalid_quantity'
        WHEN (doc_type = 'INV') <> (qty > 0) THEN 'sign_mismatch'
        WHEN unit_price IS NULL OR line_amount IS NULL OR unit_price <= 0 THEN 'invalid_amount'
        WHEN abs(qty * unit_price - line_amount) > 0.01 THEN 'amount_mismatch'
        WHEN currency IS DISTINCT FROM 'USD' THEN 'unsupported_currency'
        WHEN k.sku IS NULL THEN 'unknown_sku'
    END AS reject_reason,
    doc_date AS business_date,
    doc_no, doc_type, reference_doc, customer_id, line_no, k.sku, qty, unit_price, line_amount
FROM parsed p
LEFT JOIN ref_sku_aliases k ON k.alias = p.sku_raw
