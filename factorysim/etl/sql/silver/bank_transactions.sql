-- Bank statement lines. Statements have no transaction id, so the running balance is
-- part of the natural key. The first statement opens with a BALANCE FORWARD line.
WITH parsed AS (
    SELECT
        *,
        CAST(try_strptime(record ->> 'Date', '%m/%d/%Y') AS DATE) AS txn_date,
        trim(record ->> 'Description') AS description,
        TRY_CAST(NULLIF(regexp_replace(record ->> 'Debit', '[$,\s]', '', 'g'), '')
                 AS DECIMAL(18, 2)) AS debit,
        TRY_CAST(NULLIF(regexp_replace(record ->> 'Credit', '[$,\s]', '', 'g'), '')
                 AS DECIMAL(18, 2)) AS credit,
        TRY_CAST(regexp_replace(record ->> 'Balance', '[$,\s]', '', 'g') AS DECIMAL(18, 2))
            AS balance
    FROM bronze
)
SELECT
    _source, _file, _record_number, _arrival_date, _file_business_date, _raw, record,
    CASE
        WHEN _parse_error IS NOT NULL THEN 'malformed_record'
        WHEN txn_date IS NULL THEN 'invalid_date'
        WHEN balance IS NULL THEN 'invalid_amount'
        WHEN debit IS NOT NULL AND credit IS NOT NULL THEN 'invalid_amount'
    END AS reject_reason,
    txn_date AS business_date,
    txn_date, description,
    CASE
        WHEN description = 'BALANCE FORWARD' THEN 'opening_balance'
        WHEN description LIKE 'LOCKBOX DEPOSIT%' THEN 'customer_receipts'
        WHEN regexp_matches(description, 'PO-\d+') THEN 'supplier_payment'
        WHEN regexp_matches(description, 'WO-\d+-\d+') THEN 'maintenance_payment'
        ELSE 'unclassified'
    END AS txn_kind,
    NULLIF(regexp_extract(description, '((PO|WO)-[0-9-]+)', 1), '') AS reference,
    coalesce(credit, 0) - coalesce(debit, 0) AS amount,
    balance
FROM parsed
