# ETL design notes

The README covers what the pipeline does and how well it does it. This document covers how it works and where its limits are.

## The test bench

| Piece | Code | Sees |
|---|---|---|
| Simulator | `factorysim/simulation.py` | writes the truth (SQLite) |
| Exporter | `factorysim/sources/export.py` | truth → landing zone + defect manifest |
| Pipeline | `factorysim/etl/` | landing zone only |
| Evaluator | `factorysim/evaluation.py` | truth, manifest and lake |

The pipeline package may import only itself and third-party libraries (`tests/test_etl.py::test_pipeline_cannot_see_the_simulator`). Its knowledge of the sources comes from what a data team would really have: the file formats and the reference tables in `factorysim/etl/reference/` (code aliases, products, materials, UoM conversions).

Both the exporter and the simulator are seeded, so the landing zone is byte-for-byte reproducible (`test_export_is_deterministic`).

## Lake layout

```
lake/
  bronze/<feed>/arrival_date=YYYY-MM-DD/<file>__<sha12>.parquet     append-only
  silver/<entity>/business_month=YYYY-MM/part-0.parquet             partition overwrite
  quarantine/<feed>/business_month=YYYY-MM/part-0.parquet           partition overwrite
  gold/<table>/part-0.parquet                                       full rebuild
  _control/ingest_log/batch_id=NNNNNN.parquet                       append-only
  catalog.duckdb                                                    views, rebuilt each run
```

Bronze is partitioned by **arrival** date, because that is when the pipeline learns about data. Silver and quarantine are partitioned by **business** month, because that is how the data is queried and reconciled. A late file therefore lands in today's bronze partition but rewrites an older silver partition.

## Run model

Each `factorysim etl [--through DATE]` run does the following:

1. **Discover** landed files (optionally only those with arrival date ≤ `--through`), in arrival order.
2. **Ingest** each new file into bronze. A file whose path is already in the ingest log is skipped (a re-run). A file whose SHA-256 matches a loaded file is logged as `duplicate_content` and skipped (a re-delivery). Everything else is decoded (UTF-8, with a Windows-1252 fallback), split into records and written to bronze with lineage. Records that do not split into the header's columns are kept with `_parse_error`, and repeated header lines are dropped.
3. **Silver.** For each entity, run its SQL over all of that feed's bronze, which types, conforms and validates every record. Rows with a `reject_reason` go to quarantine. Valid rows are deduplicated on the entity's natural key, latest first. Only the business months that contain at least one record from this run's new files are written.
4. **Gold.** Read silver and quarantine back *from the lake* and rebuild the facts, the reconciliation tables and the DQ summary.
5. **Catalog.** Recreate `catalog.duckdb` with views over every table.

**Why recompute silver from all of bronze?** Deduplication and "latest revision wins" need every version of a key, and a correction for June can arrive in a July file. At about 25k records a year, a full recompute takes a second, so the run reads everything and writes only what changed. At scale the read would be bounded: either by a lookback window on arrival partitions (corrections older than N days go through a backfill), or by moving silver to a table format with `MERGE` (Iceberg, Delta or Hudi), which removes partition rewrites altogether.

**Correctness guarantees, each with a test:**
- Re-running with no new files writes nothing and leaves every table unchanged.
- Uneven incremental batches produce output identical to one full run, including a batch that straddles the MES schema change.
- Gold matches the truth exactly, except for cells that differ only because a record was unrecoverable.

## Silver entities

| Entity | Feed | Natural key | Latest wins by | Reject reasons |
|---|---|---|---|---|
| `production_events` | `mes_production` | `event_id` | revision, then arrival, file, record | malformed, missing key, bad revision/timestamp/quantity/shift/UoM, unknown station/SKU |
| `material_issues` | `mes_material_issues` | `issue_id` | arrival, file, record | as above, plus unknown material |
| `invoice_lines` | `erp_invoices` | document, line | arrival, file, record | missing key, doc type, date, quantity, sign vs. doc type, amount, `qty × price ≠ amount`, currency, unknown SKU |
| `goods_receipts` | `erp_goods_receipts` | GR number | arrival, file, record | missing key, date, quantity, amount, amount mismatch, unknown material/UoM |
| `stock_snapshots` | `erp_stock_snapshots` | date, material | arrival, file, record | date, quantity, UoM, unknown material |
| `work_order_events` | `cmms_work_orders` | WO, event, timestamp | arrival, file, record | invalid JSON, missing key, event, timestamp, cost |
| `bank_transactions` | `bank_statements` | date, description, amount, balance | arrival, file, record | date, amount |

Bank statements carry no transaction ID, so the running balance is part of the key. Two identical payments on the same day would still differ in balance.

## Time

- MES v1 (to 2025-06-30) and material issues use plant-local wall time (America/Chicago). These are converted to UTC with DuckDB's ICU time zones, and the Spark port uses `to_utc_timestamp`. The exporter never emits a local time that does not exist (the spring-forward gap).
  - **Ambiguous fall-back times (01:00–02:00 on the first Sunday of November)** are a known gap. DuckDB resolves them to the later, standard-time occurrence, and the exporter's Python `zoneinfo` writes the earlier one, so they would disagree by an hour. The ambiguity cannot change a business date, and no local timestamp in this data falls in that hour: v1 MES ends in June, and material issues happen at shift start. A real feed would need a UTC offset column or an explicit policy.
- MES v2 timestamps are UTC (`...Z`), and maintenance-system timestamps carry offsets.
- **Production day:** the night shift (C/3) runs from 22:00 to 06:00, and events after midnight belong to the day the shift started on.

## Reconciliation

Every check returns `(business_date, key, expected, actual)`, and one wrapper assigns its status:

1. `matched`: the variance is 0;
2. `explained_by_quarantine`: records from a related feed were quarantined for the same business date (or the check's date window);
3. `open_late_window`: the date is within 7 days of the latest arrival, so late files may still close the gap;
4. `exception`: none of the above.

| Check | Expected | Actual | Catches |
|---|---|---|---|
| `mass_balance` | units issued to a station | graded units produced | lost or duplicated MES records |
| `production_vs_sales` | good units produced | net units invoiced | billing errors, lost invoice lines |
| `customer_receipts` | net invoiced revenue | lockbox deposits | billing errors, missing deposits |
| `supplier_payments` | goods-receipt value | bank payment for the PO | unpaid or mis-paid receipts, UoM errors |
| `maintenance_payments` | work-order cost | bank payment for the WO | duplicate or missing completions |
| `inventory_roll_forward` | previous snapshot + receipts − issues | snapshot | lost receipts or issues, UoM errors |
| `bank_balance_continuity` | previous balance + amount | reported balance | missing or duplicated statement lines |

**Known weakness:** an explanation is coarse, because it only checks whether the same day has quarantined records from a related feed. A real billing error on such a day would be labelled *explained*. The evaluator counts these as `masked_by_quarantine` (0 in the published run). A sharper rule would compare the variance with the quarantined records' recoverable amounts, where those can be parsed.

## AWS mapping

| Local | AWS |
|---|---|
| landing directory | S3 bucket, with prefixes by feed and arrival date |
| `ingest.py` | Glue Python shell job or Lambda on `s3:ObjectCreated`; the ingest log becomes a DynamoDB table or an Iceberg table |
| silver SQL | Glue Spark jobs (`aws/glue/` has the MES one), with `partitionOverwriteMode=dynamic` and job bookmarks over bronze to find touched months |
| quarantine | S3 prefix plus a Catalog table; alerts on new reason codes |
| gold and reconciliation | Athena CTAS or views; exceptions to SNS |
| `catalog.duckdb` | Glue Data Catalog (one database per layer) |
| `factorysim etl` | Glue workflow or Step Functions on EventBridge schedule/arrival |

On Glue, job bookmarks provide only the new bronze files. The job uses them to decide which months to rebuild, then reads the full bronze for those months, because deduplication needs every version of each key. `tests/test_glue_parity.py` checks the Spark transform against the DuckDB output, row for row. The Glue entry point (`main()`) is not exercised locally and has never been deployed.

## What I would add next

- Data contracts per feed (column names, types, allowed values) checked at bronze, with an alert on schema drift instead of silent `coalesce` across versions.
- Volume and freshness monitors per feed (expected files per day, records per file).
- Quarantine replay: a corrected re-send of a quarantined record should release it automatically. Today it is simply superseded if it carries the same key.
- A larger generated dataset (more stations, multiple years) to measure run time and the cost of the full-bronze read.
