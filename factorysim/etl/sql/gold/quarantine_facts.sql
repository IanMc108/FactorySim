-- Best-effort facts recovered from quarantined records, so reconciliation can tell which
-- variances a quarantined record could account for. NULL means "not recoverable".
--
-- * business_date is the record's own date where it can be read (a back-dated line in
--   a July file belongs to June), else its file's business date.
-- * natural_key lets a quarantined record be *released* once a record with the same key
--   has loaded into silver (a corrected re-send): its data is then accounted for, and it
--   no longer explains anything.
-- * units_recovered sizes a lost MES production record of unknown quantity as the
--   station's material issued minus output recorded that day, when it is the only such
--   open record for that station and day; units_bound caps several such records together.
WITH q AS (SELECT *, TRY_CAST(record AS JSON) AS r FROM quarantine_all),
facts AS (
    SELECT
        q._source,
        q._file,
        q._record_number,
        q.reject_reason,
        sa.station_code AS station,
        ka.sku,
        m.material_code AS material,
        NULLIF(coalesce(trim(r ->> 'po_number'), trim(r ->> 'work_order'),
                        trim(r ->> 'reference_doc'),
                        regexp_extract(r ->> 'Description', '((PO|WO)-[0-9-]+)', 1)), '')
            AS reference,
        abs(strict_int(coalesce(r ->> 'quantity', r ->> 'qty'))) AS units,
        abs(money(r ->> 'line_amount')) AS amount,
        -- A bank line's deposit and payment: 0 when it is clearly the other kind.
        CASE WHEN q._source = 'bank_statements' THEN
            CASE WHEN NULLIF(trim(r ->> 'Credit'), '') IS NOT NULL THEN abs(money(r ->> 'Credit'))
                 WHEN NULLIF(trim(r ->> 'Debit'), '') IS NOT NULL
                      AND money(r ->> 'Debit') IS NOT NULL THEN 0 END
        END AS deposit,
        CASE WHEN q._source = 'bank_statements' THEN
            CASE WHEN NULLIF(trim(r ->> 'Debit'), '') IS NOT NULL THEN abs(money(r ->> 'Debit'))
                 WHEN NULLIF(trim(r ->> 'Credit'), '') IS NOT NULL
                      AND money(r ->> 'Credit') IS NOT NULL THEN 0 END
        END AS payment,
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
        q._file_business_date,
        CASE q._source
            WHEN 'mes_production' THEN NULLIF(trim(r ->> 'event_id'), '')
            WHEN 'mes_material_issues' THEN NULLIF(trim(r ->> 'issue_id'), '')
            WHEN 'erp_invoices'
                THEN NULLIF(trim(r ->> 'document_no'), '') || '#' || strict_int(r ->> 'line_no')
            WHEN 'erp_goods_receipts' THEN NULLIF(trim(r ->> 'gr_number'), '')
        END AS natural_key
    FROM q
    LEFT JOIN ref_station_aliases sa
        ON sa.alias = upper(trim(coalesce(r ->> 'station_code', r ->> 'station')))
    LEFT JOIN ref_sku_aliases ka
        ON ka.alias = upper(trim(coalesce(r ->> 'item_code', r ->> 'sku')))
    LEFT JOIN ref_materials m ON m.material_code = upper(trim(r ->> 'material'))
),
dated AS (
    SELECT
        * EXCLUDE (mes_local_ts, shift, other_ts),
        coalesce(
            CAST(mes_local_ts AS DATE)
                - CASE WHEN shift IN ('C', '3') AND hour(mes_local_ts) < 6 THEN 1 ELSE 0 END,
            CAST(other_ts AS DATE),
            _file_business_date
        ) AS business_date
    FROM facts
),
loaded_keys AS (
    SELECT DISTINCT 'mes_production' AS source, event_id AS natural_key
    FROM silver_production_events
    UNION ALL
    SELECT DISTINCT 'mes_material_issues', issue_id FROM silver_material_issues
    UNION ALL
    SELECT DISTINCT 'erp_invoices', doc_no || '#' || line_no FROM silver_invoice_lines
    UNION ALL
    SELECT DISTINCT 'erp_goods_receipts', gr_no FROM silver_goods_receipts
),
flagged AS (
    SELECT d.*, k.natural_key IS NOT NULL AS released
    FROM dated d
    LEFT JOIN loaded_keys k ON k.source = d._source AND k.natural_key = d.natural_key
),
mes_gap AS (
    SELECT i.business_date, i.station_code AS station, i.units - coalesce(p.units, 0) AS gap
    FROM (SELECT business_date, station_code, sum(qty) AS units
          FROM silver_material_issues GROUP BY ALL) i
    LEFT JOIN (SELECT business_date, station_code, sum(units) AS units
               FROM fct_production_daily GROUP BY ALL) p USING (business_date, station_code)
)
SELECT
    f.*,
    CASE WHEN f.units IS NOT NULL THEN f.units
         WHEN f._source = 'mes_production' AND NOT f.released
              AND count(*) FILTER (WHERE f.units IS NULL AND NOT f.released)
                  OVER (PARTITION BY f._source, f.business_date, f.station) = 1
             THEN g.gap
    END AS units_recovered,
    -- Upper bound on an unsized MES record's units: all open unsized records of one
    -- station and day together cannot exceed that station's gap (given to the first one).
    CASE WHEN f.units IS NOT NULL THEN f.units
         WHEN f._source = 'mes_production' AND f.station IS NOT NULL AND NOT f.released
             THEN CASE WHEN row_number() OVER (
                           PARTITION BY f._source, f.business_date, f.station,
                                        f.units IS NULL AND NOT f.released
                           ORDER BY f._file, f._record_number) = 1
                       THEN greatest(coalesce(g.gap, 0), 0) ELSE 0 END
    END AS units_bound
FROM flagged f
LEFT JOIN mes_gap g ON g.business_date = f.business_date AND g.station = f.station
