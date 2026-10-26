-- Gold facts. Each statement is `-- table: <name>` followed by one SELECT.

-- table: fct_production_daily
SELECT business_date, station_code, sku, sum(qty)::BIGINT AS units, count(*) AS events
FROM silver_production_events
GROUP BY ALL
ORDER BY ALL;

-- table: fct_material_issues_daily
SELECT business_date, station_code, material_code, sum(qty)::BIGINT AS units
FROM silver_material_issues
GROUP BY ALL
ORDER BY ALL;

-- table: fct_sales_daily
-- Net billing per day. A credit memo counts on the date of the invoice it reverses, so a
-- duplicate invoice and its later reversal cancel out on the day the duplicate was billed
-- (an unreversed duplicate stays visible there until the credit memo arrives).
WITH lines AS (
    SELECT l.*,
           coalesce(
               CASE WHEN l.doc_type = 'CM' THEN (
                   SELECT min(i.business_date) FROM silver_invoice_lines i
                   WHERE i.doc_no = l.reference_doc AND i.doc_type = 'INV') END,
               l.business_date) AS billing_date
    FROM silver_invoice_lines l
)
SELECT
    billing_date AS business_date,
    sku,
    coalesce(sum(qty) FILTER (WHERE doc_type = 'INV'), 0)::BIGINT AS invoiced_units,
    coalesce(-sum(qty) FILTER (WHERE doc_type = 'CM'), 0)::BIGINT AS credited_units,
    sum(qty)::BIGINT AS net_units,
    sum(line_amount)::DECIMAL(38, 2) AS net_revenue
FROM lines
GROUP BY ALL
ORDER BY ALL;

-- table: fct_goods_receipts
SELECT business_date AS receipt_date, gr_no, po_no, material_code, qty_received,
       uom_received, qty_base, amount::DECIMAL(18, 2) AS amount
FROM silver_goods_receipts
ORDER BY receipt_date, gr_no;

-- table: fct_maintenance
-- One row per work order. If a completion is re-sent with a correction, the newest
-- extract wins, as everywhere else (then the latest arrival, then the latest event time):
-- a correction may also move the completion time earlier.
WITH completions AS (
    SELECT *, {'extract': _file_business_date, 'arrival': _arrival_date,
               'event': event_ts_utc, 'line': _record_number} AS newest
    FROM silver_work_order_events WHERE event = 'COMPLETED'
)
SELECT
    o.work_order,
    coalesce(any_value(c.asset), any_value(o.asset)) AS asset,
    min(o.event_ts_utc) FILTER (WHERE o.event = 'OPENED') AS opened_at_utc,
    any_value(c.completed_at_utc) AS completed_at_utc,
    any_value(c.business_date) AS business_date,
    any_value(c.cost)::DECIMAL(18, 2) AS cost,
    coalesce(any_value(c.completion_events), 0) AS completion_events
FROM silver_work_order_events o
LEFT JOIN (
    SELECT work_order, arg_max(asset, newest) AS asset,
           arg_max(event_ts_utc, newest) AS completed_at_utc,
           arg_max(business_date, newest) AS business_date, arg_max(cost, newest) AS cost,
           count(*) AS completion_events
    FROM completions GROUP BY work_order
) c ON c.work_order = o.work_order
GROUP BY o.work_order
ORDER BY o.work_order;

-- table: fct_bank_transactions
SELECT
    -- statement order: a back-valued line printed on a later statement stays where it is
    row_number() OVER (ORDER BY _file_business_date, _file, _record_number) AS line_seq,
    -- stable across runs, unlike line_seq: the statement file and line it came from
    regexp_extract(_file, '[^/]+$') || '#' || _record_number AS source_line,
    txn_date, txn_kind, reference, description,
    amount::DECIMAL(18, 2) AS amount, balance::DECIMAL(18, 2) AS balance
FROM silver_bank_transactions
ORDER BY line_seq;

-- table: fct_stock_snapshots
SELECT snapshot_date, material_code, on_hand
FROM silver_stock_snapshots
ORDER BY ALL;
