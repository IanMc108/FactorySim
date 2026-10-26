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
SELECT
    business_date,
    sku,
    coalesce(sum(qty) FILTER (WHERE doc_type = 'INV'), 0)::BIGINT AS invoiced_units,
    coalesce(-sum(qty) FILTER (WHERE doc_type = 'CM'), 0)::BIGINT AS credited_units,
    sum(qty)::BIGINT AS net_units,
    sum(line_amount)::DECIMAL(18, 2) AS net_revenue
FROM silver_invoice_lines
GROUP BY ALL
ORDER BY ALL;

-- table: fct_goods_receipts
SELECT business_date AS receipt_date, gr_no, po_no, material_code, qty_received,
       uom_received, qty_base, amount::DECIMAL(18, 2) AS amount
FROM silver_goods_receipts
ORDER BY receipt_date, gr_no;

-- table: fct_maintenance
-- One row per work order. If a completion is re-sent with a correction, the latest
-- completion event (by event time) supplies the date and cost.
SELECT
    work_order,
    any_value(asset) AS asset,
    min(event_ts_utc) FILTER (WHERE event = 'OPENED') AS opened_at_utc,
    max(event_ts_utc) FILTER (WHERE event = 'COMPLETED') AS completed_at_utc,
    arg_max(business_date, event_ts_utc) FILTER (WHERE event = 'COMPLETED') AS business_date,
    arg_max(cost, event_ts_utc) FILTER (WHERE event = 'COMPLETED')::DECIMAL(18, 2) AS cost,
    count(*) FILTER (WHERE event = 'COMPLETED') AS completion_events
FROM silver_work_order_events
GROUP BY work_order
ORDER BY work_order;

-- table: fct_bank_transactions
SELECT
    row_number() OVER (ORDER BY txn_date, _file, _record_number) AS line_seq,
    txn_date, txn_kind, reference, description,
    amount::DECIMAL(18, 2) AS amount, balance::DECIMAL(18, 2) AS balance
FROM silver_bank_transactions
ORDER BY line_seq;

-- table: fct_stock_snapshots
SELECT snapshot_date, material_code, on_hand
FROM silver_stock_snapshots
ORDER BY ALL;
