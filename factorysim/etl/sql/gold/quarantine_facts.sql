-- Facts recovered from quarantined records, so reconciliation can attach the records
-- that might explain a variance to that row as evidence. NULL means "not recoverable".
--
-- * business_date is the record's own date where it can be read (a back-dated line in
--   a July file belongs to June), else its file's business date.
-- * A record is *released* once a record with the same key and at least as new (same or
--   higher MES revision, same or later extract) has loaded into silver: a corrected
--   re-send. Its data is then accounted for and it is no longer attached as evidence.
WITH q AS (SELECT *, TRY_CAST(record AS JSON) AS r FROM quarantine_all),
facts AS (
    SELECT
        q._source,
        q._file,
        q._record_number,
        q._file_business_date,
        q.reject_reason,
        sa.station_code AS station,
        ka.sku,
        m.material_code AS material,
        NULLIF(coalesce(trim(r ->> 'po_number'), trim(r ->> 'work_order'),
                        trim(r ->> 'reference_doc'),
                        regexp_extract(r ->> 'Description', '((PO|WO)-[0-9-]+)', 1)), '')
            AS reference,
        coalesce(
            utc_to_local(strict_ts(r ->> 'event_ts_utc',
                                   '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', '%Y-%m-%dT%H:%M:%SZ')),
            strict_ts(coalesce(r ->> 'event_time', r ->> 'issue_time_local'),
                      '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', '%Y-%m-%d %H:%M:%S')
        ) AS mes_local_ts,
        upper(trim(coalesce(r ->> 'shift_code', r ->> 'shift'))) AS shift,
        CASE q._source
            WHEN 'erp_invoices' THEN coalesce(
                strict_ts(r ->> 'document_date', '\d{2}/\d{2}/\d{4}', '%m/%d/%Y'),
                strict_ts(r ->> 'document_date', '\d{4}-\d{2}-\d{2}', '%Y-%m-%d'))
            WHEN 'erp_goods_receipts'
                THEN strict_ts(r ->> 'receipt_date', '\d{4}-\d{2}-\d{2}', '%Y-%m-%d')
            WHEN 'erp_stock_snapshots'
                THEN strict_ts(r ->> 'snapshot_date', '\d{4}-\d{2}-\d{2}', '%Y-%m-%d')
            WHEN 'bank_statements'
                THEN strict_ts(r ->> 'Date', '\d{2}/\d{2}/\d{4}', '%m/%d/%Y')
            WHEN 'cmms_work_orders'
                THEN utc_to_local(timezone('UTC', TRY_CAST(r ->> 'timestamp' AS TIMESTAMPTZ)))
        END AS other_ts,
        CASE q._source
            WHEN 'mes_production' THEN NULLIF(trim(r ->> 'event_id'), '')
            WHEN 'mes_material_issues' THEN NULLIF(trim(r ->> 'issue_id'), '')
            WHEN 'erp_invoices'
                THEN NULLIF(trim(r ->> 'document_no'), '') || '#' || strict_int(r ->> 'line_no')
            WHEN 'erp_goods_receipts' THEN NULLIF(trim(r ->> 'gr_number'), '')
        END AS natural_key,
        strict_int(coalesce(r ->> 'revision', r ->> 'rev')) AS revision
    FROM q
    LEFT JOIN ref_station_aliases sa
        ON sa.alias = upper(trim(coalesce(r ->> 'station_code', r ->> 'station')))
    LEFT JOIN ref_sku_aliases ka
        ON ka.alias = upper(trim(coalesce(r ->> 'item_code', r ->> 'sku')))
    LEFT JOIN ref_materials m ON m.material_code = upper(trim(r ->> 'material'))
),
loaded_keys AS (
    SELECT 'mes_production' AS source, event_id AS natural_key, revision, _file_business_date
    FROM silver_production_events
    UNION ALL
    SELECT 'mes_material_issues', issue_id, NULL, _file_business_date FROM silver_material_issues
    UNION ALL
    SELECT 'erp_invoices', doc_no || '#' || line_no, NULL, _file_business_date
    FROM silver_invoice_lines
    UNION ALL
    SELECT 'erp_goods_receipts', gr_no, NULL, _file_business_date FROM silver_goods_receipts
)
SELECT
    f.* EXCLUDE (mes_local_ts, shift, other_ts),
    coalesce(
        CAST(f.mes_local_ts AS DATE)
            - CASE WHEN f.shift IN ('C', '3') AND hour(f.mes_local_ts) < 6 THEN 1 ELSE 0 END,
        CAST(f.other_ts AS DATE),
        f._file_business_date
    ) AS business_date,
    EXISTS (
        SELECT 1 FROM loaded_keys k
        WHERE k.source = f._source AND k.natural_key = f.natural_key
          AND k._file_business_date >= f._file_business_date
          AND (f.revision IS NULL OR k.revision IS NULL OR k.revision >= f.revision)
    ) AS released
FROM facts f
