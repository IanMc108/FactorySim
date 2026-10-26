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
  _control/ingest_log/batch_id=NNNNNN/batch.parquet                 one row per file arrival
  _control/run.lock                                                 present while a run is active
  _orphaned/...                                                     bronze nobody can account for
  catalog.duckdb                                                    views, rebuilt each run
```

Bronze is partitioned by **arrival** date, because that is when the pipeline learns about data. Silver and quarantine are partitioned by **business** month, because that is how the data is queried and reconciled. A late file therefore lands in today's bronze partition but rewrites an older silver partition. As Glue and Athena expect, the partition column lives only in the path, not inside the files. Every Parquet write goes to a temporary name first and is then renamed into place.

## Run model

Each `factorysim etl [--through DATE]` run does the following:

1. **Lock and recover.** Take `_control/run.lock`, or refuse to run. **Bronze is never deleted.** Every bronze file carries its own ingest metadata (feed, file, SHA-256, arrival date and time, sequence, size, modification and change times) in its Parquet footer. A bronze file without a log entry, left by a crash between writing it and logging it or by a lost control table, has its log entry rebuilt from that metadata. A file without readable metadata is moved to `_orphaned/` for a person. A logged bronze file that has gone missing is treated as never loaded, so it is rebuilt from the landing zone; if the landing file is gone too (an S3 lifecycle rule), the run reports it as lost. A log compaction that crashed half-way (step 3) is finished first, so no log entry is counted twice. Bronze is listed once for all of this.
2. **Discover** landed files (optionally only those with arrival date ≤ `--through`), in arrival order: arrival folder, then the file's modification time (its arrival time; on S3, `LastModified`). Marker files (`_SUCCESS`, dotfiles) are ignored.
3. **Ingest** each arrival and commit it before moving on:

   | Case | Outcome |
   |---|---|
   | Same path, same size, modification time and change time as its last entry | Skipped without reading the file |
   | Name or folder that breaks the convention, or an arrival before the file's business date or more than 60 days after it | Logged `rejected`, whatever the content: a byte-identical copy of a good file in a mis-typed folder is rejected like anything else there |
   | Same path, same SHA-256 as its last entry | Logged `touched` (new size/mtime, so the next run skips the read) |
   | Content already loaded under this file name | Logged `duplicate_content` and not stored again (a re-delivery, or a revert) |
   | Bytes that decode as neither UTF-8 nor Windows-1252; an empty CSV or a missing header | Logged `rejected` with the error; the batch carries on |

   Rejected files never count towards the latest arrival. An empty JSON-lines file is not an error: it has no header, so it is how a quiet day, or a re-issue that withdraws a day, is delivered.
   | Anything else | Split into records and written to bronze with lineage |

   The change time catches a same-size correction copied with its old modification time preserved (`cp -p`, `rsync -t`).

   Records that do not split into the header's columns are kept with `_parse_error`; a short row also keeps its leading columns by position. Repeated header lines are dropped. At the end of the batch, its per-file log entries are compacted into one file.

   The **current version** of each file name is then read from the log: the `loaded` or `duplicate_content` entry with the latest arrival date, then the latest sequence. One arrival folder holds one path per file name, so within a folder the latest log entry is what is on disk now, even after an overwrite by a copy with an older modification time. The same order (`VERSION_ORDER` in `lake.py`) ranks versions everywhere, so the version silver uses is the one the file-version checks compare. Silver only sees that version's records. This one rule handles in-place overwrites, re-sends, reverts (A→B→A), header-only re-issues that withdraw every record, and versions that land in an earlier arrival folder, identically in incremental and full runs. Records with the same key in *different* files are ordered by arrival folder and then arrival time (step 4), which comes from the files, not from when a run happened to see them: a re-extract that lands in the same folder after a run wins in both. A version with fewer records than an earlier one is raised by the `file_version_shrink` check, and a revert to content that had already been replaced (A→B→A, which is also what a stale re-send after a correction looks like) by `file_version_revert`.
4. **Silver.** For each entity:
   - keep only the records of each file name's current version;
   - run the entity's SQL to type, conform and validate every record;
   - send rejected rows to quarantine, and deduplicate valid rows on the natural key, latest first (highest MES revision, then newest extract, then latest arrival folder and time);
   - fingerprint every business month (row count plus a sum of row hashes) on both the new result and the lake; rewrite the months that differ and delete months left empty.
5. **Gold.** Read silver and quarantine back *from the lake* and rebuild the facts, the reconciliation tables and the DQ summary. Compare the new reconciliation with the previous run's (`recon_changes`): which exceptions are new and which cleared.
6. **Catalog.** Recreate `catalog.duckdb` with views over every table.

`factorysim etl` prints rejected and lost files and the new and cleared exceptions. Its exit status, for a scheduler:
- **3** while any landed file stands rejected (its latest log entry is a rejection) or a current file version is lost. This is level-triggered: a retry fails again until the file is fixed or removed, and it takes precedence over 2.
- **2** with `--fail-on-new-exceptions` if this run raised exceptions the previous run did not have. This is edge-triggered, so a retry of the same run exits 0; `--fail-on-exceptions` exits 2 while any exception is outstanding.

**Why recompute silver from all of bronze?** Deduplication and "latest wins" need every version of a key, and a correction can move a record to another month: the first version of this pipeline only rewrote the months its new records fell in, and left a stale copy behind in the old month. Fingerprinting every month makes the write set exactly the months whose content changed, so there is no "touched months" logic to get wrong. The cost is reading all of bronze on every run (see *Scale and cost*).

**Correctness guarantees, each with a test:**
- Re-running with no new files reads no landing file, rewrites no silver or quarantine partition and leaves every table's content unchanged. Gold and the catalog are rebuilt, with the same content.
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

- MES v1 (to 2025-06-30) and material issues use plant-local wall time (America/Chicago). These are converted to UTC with DuckDB's ICU time zones, and the Spark port uses `to_utc_timestamp`. Every timestamp must match its exact pattern (`YYYY-MM-DD HH:MM:SS`, or `...THH:MM:SSZ` with a capital Z), because the two engines' parsers disagree on anything looser.
- **A local time that cannot be placed on the UTC timeline is quarantined**, as `nonexistent_local_time` (inside the spring-forward gap) or `ambiguous_local_time` (the fall-back hour happens twice). Both engines detect these the same way, by converting to UTC and back, ±1 hour. The exporter never emits such times; a real feed would need a UTC offset column to avoid the loss.
- MES v2 timestamps are UTC (`...Z`). Maintenance-system timestamps must carry an offset; one without is quarantined, not assumed to be UTC.
- **Production day:** the night shift (C/3) runs from 22:00 to 06:00, and events after midnight belong to the day the shift started on.

## Reconciliation

Every check returns `(business_date, key, expected, actual)`, and one wrapper assigns its status:

1. `matched`: the variance is 0;
2. `open_late_window`: the date is within 7 days of the latest arrival, so late files may still close the gap;
3. `exception`: anything else.

**Nothing is cleared automatically.** An earlier version tried to mark variances as "explained by quarantine" when the effect of the related quarantined records added up to the variance. Two rounds of review showed why that is a bad idea: the rules for what counts as related and how big an unparseable record is grew complicated, one quarantined record could "explain" two rows at once, and every mistake in them hid a real error. The pipeline now raises every variance and attaches evidence instead.

| Check | Expected | Actual | Catches |
|---|---|---|---|
| `mass_balance` | units issued to a station | graded units produced | lost or duplicated MES records, a missing MES file |
| `production_vs_sales` | good units produced | net units invoiced | billing errors, lost invoice lines, a missing MES file |
| `customer_receipts` | net invoiced revenue | lockbox deposits | billing errors, missing deposits |
| `supplier_payments` | goods-receipt value per PO (all partial deliveries) | bank payments for the PO from the first delivery to 3 days after the last | unpaid, double-paid or late-paid receipts, UoM errors; payments outside the window get their own row |
| `maintenance_payments` | work-order cost | bank payments for the WO within 3 days | duplicate or missing completions, double or late payments |
| `inventory_roll_forward` | previous snapshot + receipts − issues | snapshot | lost receipts or issues, UoM errors |
| `bank_balance_continuity` | previous balance + amount, per line in statement order (a back-valued line stays where the bank printed it), including each `BALANCE FORWARD` | reported balance | missing, duplicated or corrupted statement lines; a statement that does not carry the previous close forward |
| `unreferenced_bank_lines` | 0 | the amount of a line that is neither a deposit nor references a PO or work order | cash nobody can account for (a fraudulent or misdirected debit or credit). Only a `BALANCE FORWARD` line that moves no money is an opening balance; one with a debit or credit lands here |
| `credit_memo_references` | the referenced invoice exists | count found | credit memos for invoices that do not exist |
| `file_version_shrink` | the largest earlier record count of a file name | the current version's count | a re-issue that withdraws data (deliberate or not, a person confirms it) |
| `file_version_revert` | 0 | 1 if the current version's content had already been replaced by different content | a stale re-send that undoes a correction, or a deliberate rollback (a person confirms it) |

Bank rows are keyed by statement file and line (`bank_statement_20250304.csv#3: ...`), so a row keeps its key from run to run and `recon_changes` can track it.

**Evidence.** Each check's header in `sql/gold/recon_checks.sql` declares which feeds can affect it and how rows match (station, SKU, material, reference, or any). Every row carries `candidate_records` and `candidates`: the quarantined records from those feeds, dated in the row's window, whose key matches or could not be recovered. The window runs from `window_start` (or the row's date) to the row's date, or to the column a check names with `evidence_until`: the payment checks use it so that a quarantined payment made within the terms is attached to its PO or work order. [`sql/gold/quarantine_facts.sql`](../factorysim/etl/sql/gold/quarantine_facts.sql) recovers from each quarantined record:
- **its key and its own business date**, so a back-dated line in a July file counts for June;
- **whether it has been released**: a record with the same natural key has since loaded into silver from the same or a newer extract and, where both carry one, the same or a higher MES revision. A released record is a corrected re-send; it is no longer offered as evidence. A quarantined revision 2 is *not* released by a revision 1 that is already in silver.

Evidence helps whoever triages an exception; it changes no status. The summary table counts exceptions with and without evidence.

**How the evaluator scores it.** An injected business error is *caught* when every reconciliation row it touches is an exception, and *open* when a row is still inside the late-arrival window at the end of the run; open is reported separately and not counted as caught. Every row a lost (quarantined) record lands in must be raised, with that record attached as evidence. Any other exception is *unexpected*. The README's robustness table also runs the published seed as weekly incremental batches and reports how many exceptions were raised and later cleared (transient) and how long business errors took to be raised.

## AWS mapping

| Local | AWS |
|---|---|
| landing directory | S3 bucket, with prefixes by feed and arrival date |
| `ingest.py` | Glue Python shell job or Lambda on `s3:ObjectCreated`; the ingest log becomes a DynamoDB table or an Iceberg table |
| silver SQL | Glue Spark jobs (`aws/glue/` has the MES one), with `partitionOverwriteMode=dynamic` and job bookmarks over bronze to find touched months |
| quarantine | S3 prefix plus a Catalog table; alerts on new reason codes |
| gold and reconciliation | Athena CTAS or views, or a Glue Spark job; exceptions to SNS. The SQL is DuckDB's dialect: macros, `LATERAL` subqueries, `list()` and `arg_max` would need porting to Trino or Spark SQL |
| `catalog.duckdb` | Glue Data Catalog (one database per layer) |
| `factorysim etl` | Glue workflow or Step Functions on EventBridge schedule/arrival |

On Glue, a job bookmark over bronze only tells the job *whether* new files arrived. The job then:
1. reads the ingest log to find each file name's current version (`current_versions`);
2. recomputes silver from the whole bronze prefix;
3. syncs partitions by month fingerprint, as locally, because a correction can move a record to a month that no new file mentions.

Spark runs with `timeParserPolicy=CORRECTED`, and an empty bookmark batch is skipped. The engines differ in small ways that the job has to undo: Spark's `trim` strips only ASCII spaces where DuckDB's strips every Unicode space separator (NBSP, em space, ...), and Java's `$` matches before a trailing newline, so the job trims with an explicit `\p{Zs}` pattern and anchors with `\A...\z`. `tests/test_glue_parity.py` checks the Spark transform against the DuckDB output row for row, on three kinds of input:
- a generated half-year;
- hand-made hostile records (fractional quantities, `1e3`, fractional-second timestamps, legacy codes, DST nights, a correction that changes month);
- three seeded batches of fuzzed records.

`aws/athena/silver_production_events.sql` declares the table with partition projection, so months that partition sync adds or deletes need no `MSCK REPAIR` or crawler. Two caveats:
- **The Glue entry point (`main()`) has never run on AWS.**
- **Partition sync is overwrite-then-delete, not atomic.** A reader can see a half-updated set of months, and Iceberg would fix that.

## Scale and cost

The data here is about 25k records a year. Three things would change at production volume:

- **Ingestion is cheap; silver is O(history).** Ingestion skips unchanged landing files by size, modification and change time (on S3, the ETag), so a no-op run reads no landing data. Every run still reads all of bronze to recompute silver, so the cost of a daily run grows with history. Fixes, in order of preference:
  1. move silver to Iceberg (or Delta/Hudi) and `MERGE` changed keys, using a key-to-partition index instead of a full recompute;
  2. or bound corrections to a lookback window (for example 35 days of arrivals), with older corrections going through an explicit backfill.
- **Bronze is one small Parquet file per landed file** (about 1,650 a year, a few KB each), which is the classic small-files problem on S3. A compaction job could merge each arrival partition, but not as things stand: recovery (run model, step 1) finds a bronze file by its path and reads its ingest metadata from that file's footer, so a merged file would look like an orphan and its originals like lost bronze. Compaction needs a table format (Iceberg keeps a manifest of data files) or a recovery step that reads the per-record lineage columns instead.
- **Bronze stores each record as JSON text.** That keeps bronze schema-agnostic across the MES schema change and malformed rows (raw text is now kept only for records that failed to parse). The price is that queries on bronze parse JSON. At scale, a typed bronze per feed and schema version (or a VARIANT/struct column) would be cheaper to query.

## What I would add next

- Data contracts per feed (column names, types, allowed values) checked at bronze, with an alert on schema drift instead of silent `coalesce` across versions.
- Volume and freshness monitors per feed (expected files per day, records per file).
- Exception workflow: an owner, a comment and an "accepted" flag per reconciliation row, kept across runs, so a confirmed variance stops being re-raised.
- A larger generated dataset (more stations, multiple years) to measure run time and the cost of the full-bronze read.
