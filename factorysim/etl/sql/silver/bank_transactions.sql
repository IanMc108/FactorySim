-- Bank statement lines. Statements have no transaction id, so the running balance is
-- part of the natural key. The first statement opens with a BALANCE FORWARD line.
WITH parsed AS (
    SELECT
        *,
        CAST(strict_ts(record ->> 'Date', '\d{2}/\d{2}/\d{4}', '%m/%d/%Y') AS DATE) AS txn_date,
        trim(record ->> 'Description') AS description,
        NULLIF(trim(record ->> 'Debit'), '') AS debit_text,
        NULLIF(trim(record ->> 'Credit'), '') AS credit_text,
        money(NULLIF(trim(record ->> 'Debit'), '')) AS debit,
        money(NULLIF(trim(record ->> 'Credit'), '')) AS credit,
        money(record ->> 'Balance') AS balance
    FROM bronze
)
SELECT
    _source, _file, _record_number, _arrival_date, _file_business_date, _arrival_ts, _ingest_seq,
    _raw, record,
    CASE
        WHEN _parse_error IS NOT NULL THEN 'malformed_record'
        WHEN txn_date IS NULL THEN 'invalid_date'
        WHEN balance IS NULL THEN 'invalid_amount'
        -- A non-blank amount must parse; exactly one of debit and credit, and it must be
        -- positive (the opening BALANCE FORWARD line has neither).
        WHEN (debit_text IS NOT NULL AND debit IS NULL)
             OR (credit_text IS NOT NULL AND credit IS NULL) THEN 'invalid_amount'
        WHEN debit_text IS NOT NULL AND credit_text IS NOT NULL THEN 'invalid_amount'
        WHEN debit_text IS NULL AND credit_text IS NULL
             AND description IS DISTINCT FROM 'BALANCE FORWARD' THEN 'invalid_amount'
        WHEN debit <= 0 OR credit <= 0 THEN 'invalid_amount'
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
