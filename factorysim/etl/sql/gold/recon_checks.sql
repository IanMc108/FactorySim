-- Reconciliation checks. Each is `-- check: <name> | <related sources>` followed by one
-- SELECT returning (business_date, window_start, key, expected, actual). The pipeline
-- wraps every check with the same variance and status logic (see pipeline.py).

-- check: mass_balance | mes_production, mes_material_issues
-- Every unit of material issued to a station comes out as exactly one graded unit.
SELECT
    coalesce(i.business_date, p.business_date) AS business_date,
    NULL::DATE AS window_start,
    coalesce(i.station_code, p.station_code) AS key,
    coalesce(i.units, 0) AS expected,
    coalesce(p.units, 0) AS actual
FROM (SELECT business_date, station_code, sum(qty) AS units
      FROM silver_material_issues GROUP BY ALL) i
FULL OUTER JOIN (SELECT business_date, station_code, sum(units) AS units
                 FROM fct_production_daily GROUP BY ALL) p
    USING (business_date, station_code);

-- check: production_vs_sales | mes_production, erp_invoices
-- Good output is sold on the day it is made.
SELECT
    coalesce(p.business_date, s.business_date) AS business_date,
    NULL::DATE AS window_start,
    coalesce(p.sku, s.sku) AS key,
    coalesce(p.units, 0) AS expected,
    coalesce(s.net_units, 0) AS actual
FROM (SELECT f.business_date, f.sku, sum(f.units) AS units
      FROM fct_production_daily f JOIN ref_products r USING (sku)
      WHERE r.sellable GROUP BY ALL) p
FULL OUTER JOIN fct_sales_daily s USING (business_date, sku);

-- check: customer_receipts | erp_invoices, bank_statements
-- Each day's net invoiced revenue is deposited the same day.
SELECT
    coalesce(s.business_date, b.txn_date) AS business_date,
    NULL::DATE AS window_start,
    'ALL' AS key,
    coalesce(s.revenue, 0) AS expected,
    coalesce(b.amount, 0) AS actual
FROM (SELECT business_date, sum(net_revenue) AS revenue FROM fct_sales_daily GROUP BY ALL) s
FULL OUTER JOIN (SELECT txn_date, sum(amount) AS amount FROM fct_bank_transactions
                 WHERE txn_kind = 'customer_receipts' GROUP BY ALL) b
    ON b.txn_date = s.business_date;

-- check: supplier_payments | erp_goods_receipts, bank_statements
-- Each goods receipt is paid in full on receipt.
SELECT
    coalesce(g.receipt_date, b.txn_date) AS business_date,
    NULL::DATE AS window_start,
    coalesce(g.po_no, b.reference) AS key,
    coalesce(g.amount, 0) AS expected,
    coalesce(-b.amount, 0) AS actual
FROM fct_goods_receipts g
FULL OUTER JOIN (SELECT * FROM fct_bank_transactions WHERE txn_kind = 'supplier_payment') b
    ON b.reference = g.po_no;

-- check: maintenance_payments | cmms_work_orders, bank_statements
-- Each completed work order is paid on its completion day.
SELECT
    coalesce(m.business_date, b.txn_date) AS business_date,
    NULL::DATE AS window_start,
    coalesce(m.work_order, b.reference) AS key,
    coalesce(m.cost, 0) AS expected,
    coalesce(-b.amount, 0) AS actual
FROM fct_maintenance m
FULL OUTER JOIN (SELECT * FROM fct_bank_transactions WHERE txn_kind = 'maintenance_payment') b
    ON b.reference = m.work_order;

-- check: inventory_roll_forward | erp_stock_snapshots, erp_goods_receipts, mes_material_issues
-- Snapshot = previous snapshot + receipts - issues in between.
WITH snaps AS (
    SELECT *, lag(snapshot_date) OVER w AS prev_date, lag(on_hand) OVER w AS prev_on_hand
    FROM fct_stock_snapshots
    WINDOW w AS (PARTITION BY material_code ORDER BY snapshot_date)
)
SELECT
    s.snapshot_date AS business_date,
    s.prev_date + 1 AS window_start,
    s.material_code AS key,
    s.prev_on_hand
        + coalesce((SELECT sum(qty_base) FROM fct_goods_receipts g
                    WHERE g.material_code = s.material_code
                      AND g.receipt_date > s.prev_date AND g.receipt_date <= s.snapshot_date), 0)
        - coalesce((SELECT sum(units) FROM fct_material_issues_daily i
                    WHERE i.material_code = s.material_code
                      AND i.business_date > s.prev_date AND i.business_date <= s.snapshot_date), 0)
        AS expected,
    s.on_hand AS actual
FROM snaps s
WHERE s.prev_date IS NOT NULL;

-- check: bank_balance_continuity | bank_statements
-- Each statement line's balance equals the previous balance plus its amount.
WITH lines AS (
    SELECT *, lag(balance) OVER (ORDER BY txn_date, _file, _record_number) AS prev_balance
    FROM silver_bank_transactions
)
SELECT
    txn_date AS business_date,
    NULL::DATE AS window_start,
    'ALL' AS key,
    sum(prev_balance + amount) AS expected,
    sum(balance) AS actual
FROM lines
WHERE txn_kind <> 'opening_balance'
GROUP BY txn_date;
