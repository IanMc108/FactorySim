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
  bronze/<feed>/arrival_date=YYYY-MM-DD/<file>__<sha12>.parquet     append-only, one per file version
  silver/<entity>/business_month=YYYY-MM/part-0.parquet             synced by month fingerprint
  quarantine/<feed>/business_month=YYYY-MM/part-0.parquet           synced by month fingerprint
  gold/<table>/part-0.parquet                                       full rebuild
  _control/ingest_log/batch_id=NNNNNN/<seq>.parquet                 one entry per file version
  catalog.duckdb                                                    views, rebuilt each run
```

Bronze is partitioned by **arrival** date, because that is when the pipeline learns about data. Silver and quarantine are partitioned by **business** month, because that is how the data is queried and reconciled. A late file therefore lands in today's bronze partition but rewrites an older silver partition. As Glue and Athena expect, the partition column lives only in the path, not inside the files. Every Parquet write goes to a temporary name first and is then renamed into place.

## Run model

Each `factorysim etl [--through DATE]` run does the following:

1. **Clean up.** Delete bronze files that have no ingest-log entry (a crash between writing a file and logging it), and treat logged bronze files that have gone missing as never seen.
2. **Discover** landed files (optionally only those with arrival date ≤ `--through`), in arrival order. Marker files (`_SUCCESS`, dotfiles) are ignored.
3. **Ingest** each file version, keyed by path and SHA-256, and commit it before moving on:

   | Case | Outcome |
   |---|---|
   | Same path, same hash | Skipped (a re-run) |
   | Same hash at another path | Logged `duplicate_content` and skipped (a re-delivery) |
   | New hash at a known path, or a known file name in a later arrival folder | Loaded as a new version |
   | Name or folder that breaks the convention, or bytes that decode as neither UTF-8 nor Windows-1252 | Logged `rejected` with the error; the batch carries on |
   | Anything else | Split into records and written to bronze with lineage |

   Records that do not split into the header's columns are kept with `_parse_error`; a short row also keeps its leading columns by position. Repeated header lines are dropped.
4. **Silver.** For each entity:
   - keep only the latest version of each file name (so a re-issued file replaces its predecessor completely);
   - run the entity's SQL to type, conform and validate every record;
   - send rejected rows to quarantine, and deduplicate valid rows on the natural key, latest first;
   - fingerprint every business month (row count plus a sum of row hashes) on both the new result and the lake; rewrite the months that differ and delete months left empty.
5. **Gold.** Read silver and quarantine back *from the lake* and rebuild the facts, the reconciliation tables and the DQ summary.
6. **Catalog.** Recreate `catalog.duckdb` with views over every table.

**Why recompute silver from all of bronze?** Deduplication and "latest wins" need every version of a key, and a correction can move a record to another month: the first version of this pipeline only rewrote the months its new records fell in, and left a stale copy behind in the old month. Fingerprinting every month makes the write set exactly the months whose content changed, so there is no "touched months" logic to get wrong. The cost is reading all of bronze on every run (see *Scale and cost*).

**Correctness guarantees, each with a test:**
- Re-running with no new files writes nothing and leaves every table unchanged.
- Uneven incremental batches produce output identical to one full run, including a batch that straddles the MES schema change.
- A correction that moves a record across months leaves exactly one copy, both incrementally and in a full run.
- Gold matches the truth exactly, except for cells that differ only because a record was unrecoverable. This holds for eight different defect seeds.

## Silver entities

| Entity | Feed | Natural key | Latest wins by | Reject reasons |
|---|---|---|---|---|
| `production_events` | `mes_production` | `event_id` | revision, then newest extract (file business date), ingest sequence, record | malformed, missing key, bad revision/timestamp/quantity/shift/UoM, unknown station/SKU |
| `material_issues` | `mes_material_issues` | `issue_id` | newest extract, ingest sequence, record | as above, plus unknown material |
| `invoice_lines` | `erp_invoices` | document, line | newest extract, ingest sequence, record | missing key, doc type, date, quantity, sign vs. doc type, amount, `qty × price ≠ amount`, currency, unknown SKU |
| `goods_receipts` | `erp_goods_receipts` | GR number | newest extract, ingest sequence, record | missing key, date, quantity, amount, amount mismatch, unknown material/UoM |
| `stock_snapshots` | `erp_stock_snapshots` | date, material | newest extract, ingest sequence, record | date, quantity, UoM, unknown material |
| `work_order_events` | `cmms_work_orders` | WO, event, timestamp | newest extract, ingest sequence, record | invalid JSON, missing key, event, timestamp, cost |
| `bank_transactions` | `bank_statements` | date, description, amount, balance | newest extract, ingest sequence, record | date, amount |

"Newest extract" means the file's business date, not its arrival date, so an original file that arrives after its correction does not win. Integers must be plain digits (`10.5`, `1e3`, `1_000` and `1.0` are rejected, never rounded), and money may carry only `$`, `,` and spaces.

Bank statements carry no transaction ID, so the running balance is part of the key. Two identical payments on the same day would still differ in balance.

## Time

- MES v1 (to 2025-06-30) and material issues use plant-local wall time (America/Chicago). These are converted to UTC with DuckDB's ICU time zones, and the Spark port uses `to_utc_timestamp`. The exporter never emits a local time that does not exist (the spring-forward gap).
  - **Ambiguous fall-back times (01:00–02:00 on the first Sunday of November)** are a known gap. DuckDB resolves them to the later, standard-time occurrence, and the exporter's Python `zoneinfo` writes the earlier one, so they would disagree by an hour. The ambiguity cannot change a business date, and no local timestamp in this data falls in that hour: v1 MES ends in June, and material issues happen at shift start. A real feed would need a UTC offset column or an explicit policy.
- MES v2 timestamps are UTC (`...Z`), and maintenance-system timestamps carry offsets.
- **Production day:** the night shift (C/3) runs from 22:00 to 06:00, and events after midnight belong to the day the shift started on.

## Reconciliation

Every check returns `(business_date, key, expected, actual)`, and one wrapper assigns its status:

1. `matched`: the variance is 0;
2. `explained_by_quarantine`: the variance is what the related quarantined records would cause (below);
3. `open_late_window`: the date is within 7 days of the latest arrival, so late files may still close the gap;
4. `exception`: none of the above.

| Check | Expected | Actual | Catches |
|---|---|---|---|
| `mass_balance` | units issued to a station | graded units produced | lost or duplicated MES records |
| `production_vs_sales` | good units produced | net units invoiced | billing errors, lost invoice lines |
| `customer_receipts` | net invoiced revenue | lockbox deposits | billing errors, missing deposits |
| `supplier_payments` | goods-receipt value per PO | bank payments for the PO within 3 days | unpaid, double-paid or late-paid receipts, UoM errors; payments without a receipt get their own row |
| `maintenance_payments` | work-order cost | bank payments for the WO within 3 days | duplicate or missing completions, double or late payments |
| `inventory_roll_forward` | previous snapshot + receipts − issues | snapshot | lost receipts or issues, UoM errors |
| `bank_balance_continuity` | previous balance + amount, per line | reported balance | missing, duplicated or corrupted statement lines |

**When does quarantine explain a variance?** Each check's header in `sql/gold/recon_checks.sql` declares three things:
- which feeds can explain it;
- how rows match: by station, SKU, material or reference, or any;
- how one lost record of each feed moves the variance (for example `mes_production:+units_recovered, erp_invoices:-units`).

The pipeline recovers what it can from each quarantined record, its key and size. A lost MES production record's size is recovered from the station's material issues for that day. Then:

- **Known key:** records whose key matches the row certainly contribute their effect.
- **Unknown key:** records whose key could not be recovered *may* contribute.
- **Verdict:** the variance is explained only if it equals the certain effects plus some subset of the possible ones.
- **Unknown size:** if any related record's size is unrecoverable, the check cannot be bounded, and any variance on that row counts as explained.

The evaluator scores an injected billing error as *caught* only if it surfaces as an exception or open row. Masked errors count as missed. Across defect seeds 1–8 none were masked. The last rule above remains the known weakness: a billing error on the same day as a truncated invoice line (whose amount is lost) would be masked.

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

On Glue, a job bookmark over bronze only tells the job *whether* new files arrived. The job then recomputes silver from the whole bronze prefix and syncs partitions by month fingerprint, the same way as locally, because a correction can move a record to a month that no new file mentions. `tests/test_glue_parity.py` checks the Spark transform against the DuckDB output, row for row, on a generated half-year and on hand-made hostile records (fractional quantities, `1e3`, fractional-second timestamps, legacy codes, a DST night shift, a correction that changes month). The Glue entry point (`main()`, including partition sync) is not exercised locally and has never been deployed.

## Scale and cost

The data here is about 25k records a year. Three things would change at production volume:

- **Every run reads all of bronze**, so the cost of a daily run grows with history. Fixes, in order of preference:
  1. move silver to Iceberg (or Delta/Hudi) and `MERGE` changed keys, using a key-to-partition index instead of a full recompute;
  2. or bound corrections to a lookback window (for example 35 days of arrivals), with older corrections going through an explicit backfill.
- **Bronze is one small Parquet file per landed file** (1,651 files, a few KB each), which is the classic small-files problem on S3. A daily compaction job should merge each arrival partition, keeping the ingest log as the source of file identity.
- **Bronze stores each record as JSON text.** That keeps bronze schema-agnostic across the MES schema change and malformed rows (raw text is now kept only for records that failed to parse). The price is that queries on bronze parse JSON. At scale, a typed bronze per feed and schema version (or a VARIANT/struct column) would be cheaper to query.

## What I would add next

- Data contracts per feed (column names, types, allowed values) checked at bronze, with an alert on schema drift instead of silent `coalesce` across versions.
- Volume and freshness monitors per feed (expected files per day, records per file).
- Quarantine replay: a corrected re-send of a quarantined record should release it automatically. Today it is simply superseded if it carries the same key.
- A larger generated dataset (more stations, multiple years) to measure run time and the cost of the full-bronze read.
