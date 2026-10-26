-- Reconciliation checks. Each is
--   -- check: <name> | key=<quarantine key or any> | effect=<feed>:<effect>, ...
-- followed by one SELECT returning (business_date, window_start, key, expected, actual).
-- The pipeline wraps every check with the same variance and status logic (pipeline.py).
--
-- The header says which quarantined records may explain a variance: records from the
-- listed feeds, in the row's date window, whose recovered key (station, sku, material or
-- reference) matches the row's key or could not be recovered. <effect> is how losing one
-- such record moves the variance (actual - expected): +units, -amount, ... or ? when its
-- size cannot be known. When every related record's effect is known, the variance must
-- equal their sum exactly to count as explained; otherwise any variance is "explained"
-- (the evaluator measures how often that hides a real error).

-- check: mass_balance | key=station | effect=mes_production:?, mes_material_issues:+units
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

-- check: production_vs_sales | key=sku | effect=mes_production:+units_recovered, erp_invoices:-units
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

-- check: customer_receipts | key=any | effect=erp_invoices:+amount, bank_statements:?
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

-- check: supplier_payments | key=reference | effect=erp_goods_receipts:?, bank_statements:?
-- Each PO's receipts are paid in full within the payment terms. Payments for unknown POs,
-- or outside the terms window, are reported as their own rows so they cannot hide.
WITH receipts AS (
    SELECT po_no, min(receipt_date) AS receipt_date, sum(amount) AS amount
    FROM fct_goods_receipts GROUP BY po_no
),
payments AS (
    SELECT reference AS po_no, txn_date, -amount AS paid
    FROM fct_bank_transactions WHERE txn_kind = 'supplier_payment'
),
classified AS (
    SELECT p.*, r.receipt_date,
           r.po_no IS NOT NULL
               AND p.txn_date BETWEEN r.receipt_date
                                  AND r.receipt_date + getvariable('payment_terms_days')
               AS on_terms
    FROM payments p LEFT JOIN receipts r USING (po_no)
)
SELECT r.receipt_date AS business_date, NULL::DATE AS window_start, r.po_no AS key,
       r.amount AS expected,
       coalesce((SELECT sum(paid) FROM classified c WHERE c.po_no = r.po_no AND c.on_terms), 0)
           AS actual
FROM receipts r
UNION ALL
SELECT txn_date, NULL::DATE, po_no || ' (payment outside terms or without receipt)', 0, paid
FROM classified WHERE NOT on_terms;

-- check: maintenance_payments | key=reference | effect=cmms_work_orders:?, bank_statements:?
-- Each completed work order is paid within the payment terms of its completion day.
WITH orders AS (
    SELECT work_order, business_date, cost FROM fct_maintenance WHERE business_date IS NOT NULL
),
payments AS (
    SELECT reference AS work_order, txn_date, -amount AS paid
    FROM fct_bank_transactions WHERE txn_kind = 'maintenance_payment'
),
classified AS (
    SELECT p.*, o.work_order IS NOT NULL
               AND p.txn_date BETWEEN o.business_date
                                  AND o.business_date + getvariable('payment_terms_days')
               AS on_terms
    FROM payments p LEFT JOIN orders o USING (work_order)
)
SELECT o.business_date, NULL::DATE AS window_start, o.work_order AS key, o.cost AS expected,
       coalesce((SELECT sum(paid) FROM classified c
                 WHERE c.work_order = o.work_order AND c.on_terms), 0) AS actual
FROM orders o
UNION ALL
SELECT txn_date, NULL::DATE, work_order || ' (payment outside terms or without work order)', 0,
       paid
FROM classified WHERE NOT on_terms;

-- check: inventory_roll_forward | key=material | effect=erp_stock_snapshots:?, erp_goods_receipts:?, mes_material_issues:?
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

-- check: bank_balance_continuity | key=any | effect=bank_statements:?
-- Line by line, each statement balance equals the previous balance plus the line amount.
WITH lines AS (
    SELECT *, lag(balance) OVER (ORDER BY line_seq) AS prev_balance
    FROM fct_bank_transactions
)
SELECT txn_date AS business_date, NULL::DATE AS window_start,
       'line ' || line_seq || ': ' || description AS key,
       prev_balance + amount AS expected, balance AS actual
FROM lines
WHERE txn_kind <> 'opening_balance';
