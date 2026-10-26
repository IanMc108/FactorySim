-- Gold facts. Each statement is `-- table: <name>` followed by one SELECT.

-- table: fct_production_daily
SELECT business_date, station_code, sku, sum(qty) AS units, count(*) AS events
FROM silver_production_events
GROUP BY ALL
ORDER BY ALL;

-- table: fct_material_issues_daily
SELECT business_date, station_code, material_code, sum(qty) AS units
FROM silver_material_issues
GROUP BY ALL
ORDER BY ALL;

-- table: fct_sales_daily
SELECT
    business_date,
    sku,
    coalesce(sum(qty) FILTER (WHERE doc_type = 'INV'), 0) AS invoiced_units,
    coalesce(-sum(qty) FILTER (WHERE doc_type = 'CM'), 0) AS credited_units,
    sum(qty) AS net_units,
    sum(line_amount) AS net_revenue
FROM silver_invoice_lines
GROUP BY ALL
ORDER BY ALL;

-- table: fct_goods_receipts
SELECT business_date AS receipt_date, gr_no, po_no, material_code, qty_received,
       uom_received, qty_base, amount
FROM silver_goods_receipts
ORDER BY receipt_date, gr_no;

-- table: fct_maintenance
SELECT
    work_order,
    any_value(asset) AS asset,
    min(event_ts_utc) FILTER (WHERE event = 'OPENED') AS opened_at_utc,
    max(event_ts_utc) FILTER (WHERE event = 'COMPLETED') AS completed_at_utc,
    max(business_date) FILTER (WHERE event = 'COMPLETED') AS business_date,
    max(cost) FILTER (WHERE event = 'COMPLETED') AS cost
FROM silver_work_order_events
GROUP BY work_order
ORDER BY work_order;

-- table: fct_bank_transactions
SELECT txn_date, txn_kind, reference, description, amount, balance
FROM silver_bank_transactions
ORDER BY txn_date, _file, _record_number;

-- table: fct_stock_snapshots
SELECT snapshot_date, material_code, on_hand
FROM silver_stock_snapshots
ORDER BY ALL;
