-- Reconciliation checks. Each is
--   -- check: <name> | sources=<feeds> | key=<station|sku|material|reference|any>
--              [| evidence_until=<column>]
-- followed by one SELECT returning (business_date, window_start, key, expected, actual),
-- plus the column evidence_until names, if any.
-- The pipeline wraps every check the same way (pipeline.py): a row is matched when the
-- variance is 0, open when it is within the late-arrival window, and otherwise an
-- exception. Nothing is cleared automatically. Open quarantined records from the listed
-- feeds, dated in the row's window, whose recovered key matches the row's key (or could
-- not be recovered) are attached to the row as evidence for whoever triages it.

-- check: mass_balance | sources=mes_production,mes_material_issues | key=station
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

-- check: production_vs_sales | sources=mes_production,erp_invoices | key=sku
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

-- check: customer_receipts | sources=erp_invoices,bank_statements | key=any
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

-- check: supplier_payments | sources=erp_goods_receipts,bank_statements | key=reference | evidence_until=window_end
-- Each PO's receipts (possibly several partial deliveries) are paid in full between the
-- first delivery and the payment terms after the last. Payments for unknown POs,
-- or outside the terms window, are reported as their own rows so they cannot hide.
WITH receipts AS (
    SELECT po_no, min(receipt_date) AS receipt_date, max(receipt_date) AS last_receipt_date,
           sum(amount) AS amount
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
                                  AND r.last_receipt_date + getvariable('payment_terms_days')
               AS on_terms
    FROM payments p LEFT JOIN receipts r USING (po_no)
)
SELECT r.receipt_date AS business_date, NULL::DATE AS window_start,
       r.last_receipt_date + getvariable('payment_terms_days') AS window_end, r.po_no AS key,
       r.amount AS expected,
       coalesce((SELECT sum(paid) FROM classified c WHERE c.po_no = r.po_no AND c.on_terms), 0)
           AS actual
FROM receipts r
UNION ALL
SELECT txn_date, NULL::DATE, NULL::DATE,
       po_no || ' (payment outside terms or without receipt)', 0, paid
FROM classified WHERE NOT on_terms;

-- check: maintenance_payments | sources=cmms_work_orders,bank_statements | key=reference | evidence_until=window_end
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
SELECT o.business_date, NULL::DATE AS window_start,
       o.business_date + getvariable('payment_terms_days') AS window_end,
       o.work_order AS key, o.cost AS expected,
       coalesce((SELECT sum(paid) FROM classified c
                 WHERE c.work_order = o.work_order AND c.on_terms), 0) AS actual
FROM orders o
UNION ALL
SELECT txn_date, NULL::DATE, NULL::DATE,
       work_order || ' (payment outside terms or without work order)', 0, paid
FROM classified WHERE NOT on_terms;

-- check: inventory_roll_forward | sources=erp_stock_snapshots,erp_goods_receipts,mes_material_issues | key=material
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

-- check: bank_balance_continuity | sources=bank_statements | key=any
-- Line by line, each statement balance equals the previous balance plus the line amount.
-- A BALANCE FORWARD line after the first must carry the previous balance forward.
WITH lines AS (
    SELECT *, lag(balance) OVER (ORDER BY line_seq) AS prev_balance
    FROM fct_bank_transactions
)
SELECT txn_date AS business_date, NULL::DATE AS window_start,
       source_line || ': ' || description AS key,
       prev_balance + amount AS expected, balance AS actual
FROM lines
WHERE prev_balance IS NOT NULL;

-- check: unreferenced_bank_lines | sources=bank_statements | key=any
-- Every bank line must be a customer deposit or reference a PO or work order (whose
-- amounts the payment checks reconcile). Anything else is cash nobody can account for.
SELECT txn_date AS business_date, NULL::DATE AS window_start,
       source_line || ': ' || description AS key,
       0 AS expected, amount AS actual
FROM fct_bank_transactions
WHERE txn_kind = 'unclassified';

-- check: credit_memo_references | sources=erp_invoices | key=reference
-- Every credit memo reverses an invoice that exists.
SELECT cm.business_date, NULL::DATE AS window_start, cm.reference_doc AS key,
       1 AS expected,
       (SELECT count(DISTINCT i.doc_no) FROM silver_invoice_lines i
        WHERE i.doc_no = cm.reference_doc AND i.doc_type = 'INV') AS actual
FROM (SELECT DISTINCT doc_no, reference_doc, min(business_date) AS business_date
      FROM silver_invoice_lines WHERE doc_type = 'CM' GROUP BY ALL) cm;

-- check: file_version_shrink | sources=erp_invoices,erp_goods_receipts,erp_stock_snapshots,mes_production,mes_material_issues,bank_statements,cmms_work_orders | key=any
-- A re-issued file with fewer records than an earlier version withdraws data. That can be
-- deliberate, but a person has to confirm it (a 0-record re-issue empties the file).
SELECT file_business_date AS business_date, NULL::DATE AS window_start, file_name AS key,
       greatest(max_earlier_records, current_records) AS expected, current_records AS actual
FROM file_versions
WHERE versions > 1;

-- check: file_version_revert | sources=erp_invoices,erp_goods_receipts,erp_stock_snapshots,mes_production,mes_material_issues,bank_statements,cmms_work_orders | key=any
-- A file name whose current content had already been replaced by different content (A, B,
-- then A again). A deliberate rollback looks the same as a stale re-send that undoes a
-- correction, so a person confirms it.
SELECT file_business_date AS business_date, NULL::DATE AS window_start, file_name AS key,
       0 AS expected, 1 AS actual
FROM file_versions
WHERE reverted;
