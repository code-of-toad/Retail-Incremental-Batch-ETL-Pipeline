# Retail Incremental Batch ETL Pipeline

A two-day retail data engineering capstone using **Python, PySpark, and Spark SQL** to practise incremental loading, data quality, deduplication, idempotency, testing, and recovery.

**Goal:** Build one small pipeline and explain its correctness, execution, failure modes, and design tradeoffs in a technical interview.

**Status:** Day 1 scaffold supplied: fixed seed files, setup, function stubs, eight SQL prompts, and test skeletons. ETL logic is intentionally unimplemented. Setup tests are separate from skipped learner exercises. All project documentation stays in this README.

<a id="top"></a>

## Contents

- [Business scenario and architecture](#business-scenario-and-architecture)
- [Data contracts](#data-contracts)
- [Seed expectations](#seed-expectations)
- [Project structure and setup](#project-structure-and-setup)
- [Two-day plan](#two-day-plan)
- [Implementation exercises](#implementation-exercises)
- [SQL exercises](#sql-exercises)
- [Spark investigations](#spark-investigations)
- [DE review and interview checkpoints](#de-review-and-interview-checkpoints)
- [Definition of done](#definition-of-done)

## Business scenario and architecture

A retailer receives daily sales files. Deliveries contain new order lines, repeated records, corrections, invalid rows, and sales arriving after their business date. Reporting needs reliable revenue, units, order counts, and margin by date, store, and product.

Build a **daily incremental batch ETL pipeline**, running locally with static product/store dimensions and Parquet output. Incremental inputs update a small complete sales snapshot; rebuilding that snapshot is an intentional local simplification.

```mermaid
flowchart TD
    A["Immutable daily CSV deliveries"] --> B["Parse and validate"]
    D["Product and store dimensions"] --> B
    B --> Q["Quarantine with reasons"]
    B --> V["Resolve valid record versions"]
    V --> U["Apply inserts and corrections"]
    P["Previously committed sales snapshot"] --> U
    U --> S["Stage Parquet and reconcile"]
    S --> C["Publish committed snapshot"]
    C --> R["Spark SQL reporting"]
    C --> M["Commit metadata and batch history"]
```

Log counts and outcomes throughout. Readers use the committed snapshot reference; uncommitted staging output is not reportable data. Publication and batch history must refer to the same successful result.

**Scope:** Exactly 150 sales rows across three simulated delivery dates, 10 products, and 5 stores. These deliveries are processed within two study days. No scale-up generator, cloud deployment, streaming implementation, or orchestration platform.

**Architecture boundary:** This is a warehouse-style analytical model on local files. Raw/validated/curated layers support data lake organization. Plain Parquet does not provide transactional `MERGE`; warehouse and lakehouse equivalents are design topics. A local snapshot workflow is not a distributed transaction system.

[Back to top](#top)

## Data contracts

### Datasets and grain

| Dataset | Size / grain | Key and purpose |
| --- | --- | --- |
| `products.csv` | 10 rows; one product | `product_id`; name and category |
| `stores.csv` | 5 rows; one store | `store_id`; name and province |
| `sales_2026-04-01.csv` | 60 source rows; one delivered order-line version | Initial load, invalid rows, duplicates |
| `sales_2026-04-02.csv` | 50 source rows; one delivered order-line version | New lines, corrections, repeated and stale versions |
| `sales_2026-04-03.csv` | 40 source rows; one delivered order-line version | Late arrivals, conflicts, recovery and replay |
| `dim_product`, `dim_store` | One row per product/store | Unique, non-null reference keys |
| `fact_sales` | One current accepted version per order line | Composite business key `(order_id, order_line_id)` |
| `quarantine` | One rejected source row occurrence | Source identity plus an array of rejection reasons |

`S005` has two cancelled lines and no completed sales across all batches. Household products `P009` and `P010` each contribute `10.00` revenue, providing a ranking tie.

### Sales fields

| Fields | Target Spark types | Contract |
| --- | --- | --- |
| `order_id`, `store_id`, `product_id` | `StringType` | Required, trimmed, nonblank |
| `order_line_id` | `IntegerType` | Positive; unique within an order in the final fact |
| `sale_date` | `DateType` | Business date; may precede delivery date |
| `quantity` | `IntegerType` | Positive, including cancelled lines |
| `unit_price`, `unit_cost`, `discount_amount` | `DecimalType(12, 2)` | Nonnegative; discount is for the whole line |
| `order_status` | `StringType` | `COMPLETED` or `CANCELLED` |
| `updated_at` | `TimestampType` | Required source version timestamp; interpret in UTC |

Dimensions contain required strings: `product_id`, `product_name`, `category`; and `store_id`, `store_name`, `province`.

Each sales CSV supplies `source_row_id` (`R001`, `R002`, …), unique within that file. Attach `batch_id`, `run_id`, and `source_file`. Stable occurrence identity is `(batch_id, source_file, source_row_id)`; `run_id` identifies an attempt. Duplicate business payloads intentionally have different source-row IDs. Do not use partition-dependent IDs for replay identity.

**Parsing:** Preserve source values so missing and malformed inputs can be diagnosed separately. Define explicit schemas and parsing rules; do not rely on inferred CSV types or assume schema declaration enforces business constraints. Structurally unreadable deliveries fail before publication.

### Business and version rules

- Require every sales field; reject invalid types, domains, ranges, missing references, and discounts exceeding `quantity × unit_price`. Dimension key defects fail the batch because they make enrichment unsafe.
- An order belongs to one store and business date. Fixtures preserve that relationship; report distinct orders using `order_id`, not order-line count.
- Completed lines: `gross_sales = quantity × unit_price`; `net_sales = gross_sales − discount_amount`; `gross_margin = net_sales − quantity × unit_cost`. Cancelled lines remain in the fact with zero realized sales, margin, and completed units. Negative margin is allowed. Choose explicit decimal result precision; avoid floating-point money.
- Separate exact duplicates from different versions. Collapse identical business payloads even when delivery metadata differs. Retain accounting for every input occurrence.
- For a business key, a newer `updated_at` supersedes an older accepted version. Equal timestamp plus equal payload is a replay. Equal timestamp plus different payload is a conflict: if any validated incoming versions disagree at the same timestamp, or disagree with the committed version at its timestamp, quarantine all valid incoming occurrences for that business key and preserve any committed version. This conservative rule also applies when that key has another, newer incoming version. Do not invent version precedence from file order or random IDs. Among identical payload copies, choosing the smallest stable source occurrence ID for lineage is permitted; it does not decide business precedence.
- Validate before applying updates. Invalid newer records cannot erase valid committed data. Cancelled status is an update, not a physical deletion; source deletes are outside this implementation.
- Discover work by delivery/batch identity, not `sale_date > last_processed_date`. Late facts and corrections can change historical totals. Explain when a source update-time high-water mark would need overlap and deduplication.

[Back to top](#top)

## Seed expectations

These are acceptance targets, not outputs from an implemented pipeline. Money is CAD. Process the batches in delivery order for the tables below; reruns must preserve the resulting business state. `tests/fixtures/expected_batches.json` contains the same expected values for your assertions.

| Delivery | Source | Validation rejects | Conflict rejects | Ignored | Inserts | Updates | Final fact rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-04-01 | 60 | 8 | 2 | 6 | 44 | 0 | 44 |
| 2026-04-02 | 50 | 4 | 3 | 5 | 32 | 6 | 76 |
| 2026-04-03 | 40 | 4 | 3 | 5 | 24 | 4 | 100 |

| After delivery | Completed lines | Completed orders | Completed units | Gross sales | Net sales | Gross margin |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-04-01 | 42 | 41 | 43 | 430.00 | 429.00 | 171.00 |
| 2026-04-02 | 73 | 72 | 76 | 765.00 | 762.00 | 300.00 |
| 2026-04-03 | 97 | 96 | 102 | 1027.00 | 1024.00 | 406.00 |

**Named cases:** All source-row IDs below refer to the indicated delivery file.

| Delivery / source rows | Purpose and expected behavior |
| --- | --- |
| Apr 1 `R001`–`R044` | 44 accepted business keys. `O1001` has two lines; its line 1 yields gross/net/margin `20.00 / 19.00 / 7.00`. |
| Apr 1 `R045`–`R050` | Four duplicate payload copies and two superseded versions; six ignored occurrences. |
| Apr 1 `R051`–`R052` | Two conflicting versions of new key `O1090`; quarantine both, insert neither. |
| Apr 1 `R053`–`R060` | Missing order ID, blank product ID, malformed quantity, malformed date, unknown product, unknown store, negative price, multiple violations. |
| Apr 2 `R001`–`R032` | 32 new sales lines. |
| Apr 2 `R033`–`R038` | Six corrections: price, quantity, cancellation, discount, cost, price. `O1006` gains negative margin; it remains valid. |
| Apr 2 `R039`–`R043` | Duplicate/replayed/stale occurrences; ignore all five. |
| Apr 2 `R044`–`R046` | `O1011` has two conflicting incoming versions; `O1012` disagrees with committed data at the same timestamp. Preserve prior versions. |
| Apr 2 `R047`–`R050` | Missing timestamp, malformed price, excessive discount, invalid newer quantity for `O1013`. That invalid update must not erase its committed row. |
| Apr 3 `R001`–`R024` | 22 new current-date lines plus late April 1 orders `O3901`/`O3902` at `R023`/`R024`. |
| Apr 3 `R025`–`R028` | Four corrections, including cancellation and reinstating a previously cancelled line. |
| Apr 3 `R029`–`R033` | Five ignored repeated/stale occurrences. |
| Apr 3 `R034`–`R036` | Two incoming conflicts for `O2005`, one conflict against current `O2006`. |
| Apr 3 `R037`–`R040` | Missing store, malformed line ID, zero quantity, malformed timestamp. |

**Parsing and rejection conventions:** Retain the original eleven values as `raw_<field>` alongside typed fields; add `parse_errors: array<string>`. Use `MISSING_<FIELD>` for null/blank source values and `MALFORMED_<FIELD>` for nonblank values that cannot become the required type. A malformed value is not also missing. Integers are whole-number text; monetary text has at most two decimal places; dates use `yyyy-MM-dd`, timestamps use `yyyy-MM-dd'T'HH:mm:ss` in UTC. The fixtures contain no currency symbols or fractional cents.

Use `NONPOSITIVE_ORDER_LINE_ID`, `NONPOSITIVE_QUANTITY`, `NEGATIVE_UNIT_PRICE`, `NEGATIVE_UNIT_COST`, `NEGATIVE_DISCOUNT_AMOUNT`, `INVALID_ORDER_STATUS`, `UNKNOWN_PRODUCT_ID`, `UNKNOWN_STORE_ID`, and `EXCESSIVE_DISCOUNT` for the corresponding rules. Check unknown references only for present nonblank IDs, and excessive discount only when quantity/price/discount already satisfy their individual ranges. Quarantine arrays contain all applicable reasons, without null elements or duplicate reason strings. Version conflict reason: `CONFLICTING_VERSION`.

Apr 1 `R060` must have exactly: `MISSING_ORDER_ID`, `NONPOSITIVE_QUANTITY`, `UNKNOWN_PRODUCT_ID`, `NEGATIVE_UNIT_COST`, `INVALID_ORDER_STATUS` (reason order does not matter). Accepted candidates retain metadata/raw evidence and an empty reason array. Derived fact measures use `DecimalType(18, 2)` and `completed_units` uses `IntegerType`.

**Version-stage interfaces:** `resolve_versions` receives validated typed incoming rows and committed source fields (an empty DataFrame with a compatible schema for Day 1). Candidates contain one winning occurrence per changed key plus `change_type` (`INSERT`/`UPDATE`). Ignored occurrences carry `ignore_reason`; conflicts carry `rejection_reasons`. `apply_upserts` produces current source state; `build_fact_sales` enriches that state and computes measures. Payload comparison includes the eleven source business fields; it excludes metadata, raw evidence, reason arrays, and dimension/derived attributes.

**Python fixture targets:** In `PRACTICE_RECORDS`, key counts are `{('O1001', 1): 3, ('O1002', 1): 2, ('O1003', 1): 1}`. Duplicate keys are `O1001/1` and `O1002/1`; latest selection returns winners `O1001/1` at 10:00 priced `12.00`, and `O1003/1` priced `7.00`, plus both `O1002/1` conflict occurrences. Sorting is ascending by `(updated_at, order_id, order_line_id)`; equal sort keys retain input order. Bounds return first encountered among fully tied keys, not a resolution of conflicting payloads. Only `sort_in_place` mutates its input; it returns `None`.

[Back to top](#top)

## Project structure and setup

| Path | Responsibility |
| --- | --- |
| `README.md`, `.gitignore` | Central specification; exclude runtime output and local secrets |
| `requirements.txt`, `pyproject.toml` | Pinned dependencies, editable package install, pytest configuration |
| `data/seed/` | Five fixed CSVs: products, stores, three sales deliveries |
| `src/retail_etl/config.py`, `session.py`, `seed_io.py` | Supplied paths/constants, local Spark setup, tiny CSV fixture loader |
| `src/retail_etl/python_practice.py` | Six Python exercises and local practice records |
| `src/retail_etl/schemas.py`, `ingestion.py` | Supplied raw/dimension schemas; learner typed schema, reads and parsing |
| `src/retail_etl/validation.py`, `transformations.py` | Quality rules, quarantine, enrichment, measures, aggregation TODOs |
| `src/retail_etl/incremental.py`, `reconciliation.py` | Version resolution, upserts, reconciliation TODOs |
| `src/retail_etl/storage.py`, `pipeline.py` | Publication/orchestration TODOs; supplied smoke-test CLI |
| `sql/01_filter.sql` … `08_quality.sql` | Eight query prompts without query solutions |
| `tests/conftest.py`, `test_setup.py` | Supplied Spark/seed fixtures and six setup checks |
| `tests/test_*.py`, `tests/fixtures/expected_batches.json` | 22 skipped exercise tests and fixed expected results |
| `runtime/`, `_scratch/` | Generated output and disposable experiments; created only when needed |

**Environment:** Python 3.10+ for this scaffold; pinned PySpark `4.0.1` and pytest `8.4.2`. Use Java 17 or later and configure `JAVA_HOME` for local Spark. These are fixed project pins, not a claim about the newest releases. See the [versioned PySpark installation requirements](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html).

In **PowerShell**, open the extracted repository root and run:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m retail_etl.pipeline --smoke-test
.\.venv\Scripts\python.exe -m pytest -q
```

Calling the environment's interpreter directly avoids activation-policy problems. In VS Code, select `.venv\Scripts\python.exe` as the interpreter. The editable install makes `retail_etl` importable without changing `PYTHONPATH`.

**Verification:** Setup was checked on Linux with Python 3.12, Java 17, and the pinned dependencies. Windows commands are supplied; run the smoke test on your own machine before starting.

**Expected now:** Smoke test prints `SETUP_OK` and `parquet_rows: 3`; pytest reports **6 passed, 22 skipped**. Skipped tests are unfinished exercises, not evidence of a working pipeline. The smoke test starts Spark, writes/reads temporary Parquet, and stops the session. If it fails, resolve the Python/Java/path error before implementation; the package does not bundle Java or Windows Hadoop native binaries.

After implementing the pipeline:

```powershell
.\.venv\Scripts\python.exe -m retail_etl.pipeline --batch-id 2026-04-01
.\.venv\Scripts\python.exe -m retail_etl.pipeline --batch-id 2026-04-02
.\.venv\Scripts\python.exe -m retail_etl.pipeline --batch-id 2026-04-03
```

Before implementation, batch commands deliberately return `EXERCISE_NOT_IMPLEMENTED` and exit code `2`. `--fail-at after_stage` and `--fail-at after_publish` define the Day 2 injection points; the failure behavior is yours to implement.

**Start here:** Open `python_practice.py`, implement only `count_keys`, remove the skip marker on `test_count_keys`, and write its assertions. Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_python_practice.py -k count_keys -q
```

Then complete the Python exercises, `sales_schema`, ingestion/parsing, validation, initial version resolution with empty current state, and fact construction. Add tests as each function becomes usable. Day 2 extends version handling to existing state and adds publication/recovery.

Use single-quoted Python strings and short what/why comments. Keep transformations separate from I/O. Preserve seeds; write generated data under `runtime/`. The four local shuffle partitions are a small-data starting point, not a production tuning recommendation.

[Back to top](#top)

## Two-day plan

**16 focused hours; breaks are additional.** Keep each timebox. Record unfinished work honestly and preserve the interview practice time.

| Day | Work | Time |
| --- | --- | ---: |
| 1 | Scenario, architecture, grain, warehouse/lake/lakehouse | 45 min |
| 1 | Python structures, duplicates, sorting, lambdas, complexity | 60 min |
| 1 | Ingestion, schemas, validation, quarantine, transformations | 150 min |
| 1 | SQL exercises | 90 min |
| 1 | Initial tests, reconciliation, spoken walkthrough | 75 min |
| 1 | Rapid DE questions and weak-answer review | 60 min |
| 2 | Incremental versions, corrections, late arrivals, idempotency | 120 min |
| 2 | Failure recovery, retries, backfills, publication, observability | 75 min |
| 2 | Spark execution and performance investigations | 75 min |
| 2 | Broader DE concepts and architecture scenarios | 90 min |
| 2 | Project walkthrough and behavioral examples | 30 min |
| 2 | Mock interview | 45 min |
| 2 | Repair gaps exposed by the mock | 45 min |

**Day 1 exit:** Explain the contracts; produce a validated initial fact and quarantine; run SQL and basic correctness checks. **Day 2 exit:** Process all deliveries, demonstrate replay/recovery, inspect plans, and complete the mock.

[Back to top](#top)

## Implementation exercises

The signatures below define interfaces, not solutions. Starter files and concrete seed fixtures are included. For each exercise, write the logic and meaningful test assertions yourself.

### 1. Python reasoning and lambdas

**Requirement / inputs → outputs:** From a list of retail record dictionaries, count occurrences of composite keys, identify duplicate keys with sets, and select the newest record per key under the conflict rule. Use parsed timestamps in local comparisons.

**Interfaces:** `count_keys(records) -> dict`; `find_duplicate_keys(records) -> set`; `select_latest(records) -> tuple[list, list]` returning winners and conflicts.

**Fixture:** Key `('O1001', 1)` appears twice identically at `09:00`, then with a correction at `10:00`; another key has different prices at the same timestamp. Add an empty list and a one-record list.

**Acceptance:** Empty input works; inputs remain unchanged; duplicates, newer versions, and conflicts are distinguished. Practise `sorted(..., key=...)`, `list.sort(key=...)`, `min`/`max` with `key=`, comprehensions, tuple keys, functions, and exceptions. Write single-field and tuple-returning lambdas yourself.

**Interview:** What does `key=` receive and return? When is `def` clearer? How do hash-based grouping and sorting differ in time/space cost? How does a Python lambda over local values differ from one building a Spark `Column` expression, or from a Python UDF?

### 2. Ingestion and data quality

**Requirement / inputs → outputs:** Raw sales plus dimensions → explicitly typed rows, validated candidates, and quarantine with all applicable reasons.

**Interfaces:** `read_batch(spark, path, batch_id, run_id) -> DataFrame`; `parse_sales(raw_df) -> DataFrame`; `validate_dimensions(products_df, stores_df) -> None`; `validate_sales(sales_df, products_df, stores_df) -> tuple[DataFrame, DataFrame]`.

**Fixture:** Null key, whitespace ID, malformed quantity/date, unknown product/store, negative amount, excessive discount, and a row violating several rules. Include duplicate dimension keys.

**Acceptance:** No silent record loss; malformed and missing values are distinguishable; accepted rows satisfy rules; quarantine retains raw evidence and reasons. Check both single and composite keys. Duplicate dimensions fail before fact enrichment. Version duplicates follow the separate resolution policy.

**Interview:** Why is a declared schema insufficient? How do SQL null semantics affect rules? Why can dimension duplicates inflate revenue? How would you handle schema drift?

### 3. Initial analytical model

**Requirement / inputs → outputs:** Valid, resolved Day 1 candidates plus dimensions → unique order-line fact and date/store reporting totals.

**Interfaces:** `build_fact_sales(sales_df, products_df, stores_df) -> DataFrame`; `aggregate_daily_sales(fact_df) -> DataFrame`.

**Fixture:** Completed and cancelled sales; a multi-line order; an unsold store. One completed line has quantity `2`, price `10.00`, cost `6.00`, discount `1.00`: expected gross/net/margin are `20.00 / 19.00 / 7.00`.

**Acceptance:** Enrichment preserves fact grain; money matches hand calculations; cancelled lines contribute zero realized metrics. Compare one DataFrame result with equivalent Spark SQL. Avoid counting a multi-line order more than once.

**Interview:** Which keys are natural, composite, or surrogate? Why avoid a fresh random surrogate on every replay? Which joins/aggregations can shuffle? What changes if product attributes need history?

### 4. Incremental loading and idempotency

**Requirement / inputs → outputs:** Existing fact plus a new validated delivery → updated fact and per-row outcome counts.

**Interfaces:** `resolve_versions(incoming_df, current_df) -> tuple[DataFrame, DataFrame, DataFrame]` returning candidates, conflicts, and ignored occurrences; `apply_upserts(current_df, candidates_df) -> DataFrame`.

**Fixture:** New key, newer correction, stale correction, exact replay, equal-time conflict, and an April 1 sale delivered April 3. Reuse the same fixtures for initial loading with an empty current fact.

**Acceptance:** One fact row per key; newer corrections replace values; stale/repeated rows do not change state; conflicts preserve committed data. Replaying any delivery yields the same business snapshot and totals. Batch IDs identify immutable contents; changed contents under an existing ID must fail. Verify that a late arrival changes its historical reporting date.

**Interview:** Why is append insufficient? How is deduplication different from idempotency? What are insert/update/no-op conditions in a warehouse `MERGE`? Why can an event-date watermark miss data?

### 5. Publication, recovery, and observability

**Requirement / inputs → outputs:** A reconciled candidate snapshot → a committed version, recoverable batch history, and structured run metrics.

**Interfaces:** `publish_snapshot(fact_df, batch_id, run_id, output_root, batch_metadata, fail_at=None) -> dict`; `run_batch(batch_id, fail_at=None) -> dict`.

**Fixture:** Inject failure after staging but before publication, then after publication but before success logging. Retry the same delivery; backfill an older delivery.

**Acceptance:** Readers continue using the previous committed result when staging fails. Never overwrite the snapshot being lazily read. Batch completion advances only with a successful commit; restart can derive completion from committed metadata even if final logging failed. Document the local publication mechanism and its filesystem assumptions here; do not claim distributed atomicity or concurrent-writer safety.

**Metrics:** `batch_id`, `run_id`, status, duration, source rows, validation rejects, version conflicts, ignored occurrences, inserts, updates, final rows, net sales, committed version, source update maximum, delivery delay, and last successful commit time. Distinguish event freshness from pipeline freshness; a late sale is not itself a processing failure.

**Interview:** Where is the commit boundary? Which failures are retryable? How do you avoid advancing state too soon? How would transactional tables change recovery and concurrent writes?

### 6. Tests and reconciliation

**Requirement / inputs → outputs:** Small deterministic fixtures and batch results → passing assertions or actionable failures. Use a reusable session-scoped Spark fixture and isolated temporary output directories.

**Interface:** `reconcile(before_df, after_df, outcomes) -> dict`; tests follow `test_<behavior>(spark, tmp_path)` where needed.

**Acceptance:** Cover schema, multiple rejection reasons, duplicate keys, RI, decimal measures, version precedence, equal-time conflict, late arrivals, replay, and failure/retry. Compare DataFrames without relying on row order; preserve multiplicities and compare schemas. Include an end-to-end three-delivery test.

Use mutually exclusive outcomes for row accounting:

`source occurrences = validation rejects + version-conflict rejects + ignored occurrences + inserts + updates`

`final fact rows = prior fact rows + inserts` (this project has no physical deletes).

`final net sales = prior net sales + inserted net sales + sum(new − old net sales for updates)`.

Ignored occurrences include duplicate copies, superseded versions, and stale/repeated candidates. A row with several rejection reasons counts once. Logs may gain attempts on replay; business state must remain stable.

**Interview:** Why is a row-count check insufficient? What distinguishes unit and integration tests? How can a set comparison hide duplicates? Which assertion would catch a many-to-many join?

[Back to top](#top)

## SQL exercises

Register `fact_sales`, `dim_product`, `dim_store`, `incoming_sales`, and `current_sales` as temporary views. Version-analysis views contain typed source fields. Use Spark SQL; warehouse `MERGE` remains conceptual. Each numbered row is one query-file exercise and may contain short subqueries/tasks. After creating your DataFrames, register views and run one completed statement at a time in the same Spark session:

```python
from pathlib import Path

fact_df.createOrReplaceTempView('fact_sales')
products_df.createOrReplaceTempView('dim_product')
stores_df.createOrReplaceTempView('dim_store')
# Register incoming/current typed views similarly when you reach Exercise 7.
spark.sql(Path('sql/02_stores.sql').read_text(encoding='utf-8')).show()
```

The supplied `.sql` files contain comments only; write a query before executing them. Put the day's checked fact in `fact_df`; final snapshot expectations apply only after processing all three deliveries.

| # | Requirement and input | Expected output / grain | Acceptance and interview check |
| --- | --- | --- | --- |
| 1 | Filter `fact_sales` by date/status; practise `CASE`, `COALESCE`, `IS NULL` | Order key, date, status, net sales; one selected line | Explicit date boundaries. When does replacing null with zero hide bad data? |
| 2 | Left join stores to completed sales; also explain an inner join | Store ID, revenue, completed-line count; one row per store | Include all 5 stores, with zero for unsold stores. Why do `WHERE` vs `ON` and `COUNT(*)` vs `COUNT(column)` matter? |
| 3 | Conditional aggregation over facts; practise `GROUP BY` and `HAVING` | Date/store, revenue, units, distinct orders, cancelled-line count | Reconcile totals; distinguish line/order counts. How does `HAVING` differ from `WHERE`? |
| 4 | CTEs and pre-aggregation for average completed order value | Store ID, completed-order count, average order value | Multi-line orders count once; cancelled lines add no revenue. Why is average line value wrong? |
| 5 | Aggregate products within category, then rank | Category, product ID, revenue, `ROW_NUMBER`, `RANK`, `DENSE_RANK` | Revenue ties remain ties for rank functions; row-number selection has deterministic ordering. Which ranking fits “top 2 including ties”? |
| 6 | Daily store totals with `LAG` and cumulative revenue | Date/store, revenue, prior observed-date revenue, change, cumulative revenue | Explicit cumulative window frame; no invented missing dates. Is the prior observed date necessarily yesterday? |
| 7 | Analyse valid incoming/current versions with CTEs and windows | Business key, candidate timestamp, outcome | Match the incremental conflict/precedence policy; classify inserts, updates, repeats, stale versions, and conflicts. Why is `DISTINCT` insufficient? |
| 8 | Quality and reconciliation queries over facts/dimensions | Violating keys/references; summary counts and totals | Detect duplicate composite keys and orphans; reproduce reconciliation. How does pre-aggregation prevent join fan-out? |

[Back to top](#top)

## Spark investigations

Use the small data, `explain('formatted')`, and the local Spark UI. Record observed plans and metrics in this README during implementation. Timing differences on these inputs are not evidence of production speedups.

| Investigation | Evidence to collect | Explain |
| --- | --- | --- |
| Execution anatomy | Follow an action through SQL, Jobs, Stages, and task counts | Lazy evaluation; driver/executors; narrow/wide operations; shuffle boundaries. One action can involve multiple jobs. |
| Join strategy | Compare broadcast and sort-merge plans under deliberate experiment settings; confirm actual operators | Broadcast exchange vs key shuffles; join cardinality; broadcast memory limits; restore settings afterward |
| Parquet reads | Inspect read schema, pushed filters, and partition filters on unpartitioned and date-partitioned experiment outputs | Column pruning vs predicate pushdown vs partition pruning; partitioning and small-file costs |
| Partition changes | Inspect `repartition` and `coalesce`, Exchange nodes, and output files | Storage/input/execution/shuffle/output partitions; balancing versus reducing parallelism |
| AQE, skew, memory | Compare initial/final adaptive plans when visible; interpret hypothetical task and shuffle metrics | AQE coalescing/skew handling; hot keys vs non-skew stragglers; spill/GC; caching only justified reuse |

Keep the core fact snapshot unpartitioned for this tiny dataset. Use a separate disposable output for the partition-pruning experiment. Discuss scale and skew without generating larger data or claiming effects the experiment did not show.

[Back to top](#top)

## DE review and interview checkpoints

**After every major implementation, answer aloud:** Why this approach and what alternative? What is the input/output grain? What triggers Spark work and shuffles? What proves correctness? What happens on failure or replay? Is it idempotent? What breaks at 100× volume? What tradeoff follows?

| Area | Questions to practise |
| --- | --- |
| Architecture | ETL vs ELT? Batch vs streaming? What volume, latency, consumers, update patterns, and recovery objectives must you clarify first? |
| Modeling and storage | OLTP vs OLAP? Normalization vs star schema? Natural/composite/surrogate keys? Lake vs warehouse vs lakehouse? SCD Type 1 vs Type 2 and historical fact joins? |
| Ingestion | Files vs APIs vs database extracts? What does CDC capture? How do contracts, schema drift, event time, arrival time, and source update time differ? |
| Reliability | Transactions and atomicity? At-least-once delivery with idempotent outcomes? Checkpoints vs batch history? Replay vs retry vs backfill? What evidence supports an exactly-once claim? |
| Operations | Scheduling and dependencies? Unit/integration checks before deployment? Configuration and secrets? Freshness, reconciliation, lineage, least privilege, sensitive data, and cost controls? |
| Scaling | Large snapshots, small files, skew, executor memory, broadcast limits, and shuffle costs? What would you change before moving to a warehouse or transactional table format? |
| Communication | Give a two-minute pipeline walkthrough. Explain a bug, evidence used, fix, and tradeoff. Prepare truthful examples of ambiguity, learning, debugging, and collaboration. |

Warehouse modeling and analytical queries are local implementation exercises. SCDs, warehouse `MERGE`, and transactional lakehouse capabilities—atomic commits, consistent reads, concurrency control, schema evolution, and history—remain conceptual. Keep CDC, orchestration, security, and deployment review at interview depth.

**Assistance ladder:** Conceptual hint → relevant API → pseudocode → partial code → complete solution only on explicit request.

### Final 45-minute mock interview

| Segment | Time | Task |
| --- | ---: | --- |
| Python | 7 min | Solve a small record/dictionary problem; explain a lambda and complexity |
| SQL | 8 min | Write a join/window query and defend grain/cardinality |
| PySpark | 10 min | Explain a plan, shuffle, join choice, and performance diagnosis |
| Architecture and capstone | 12 min | Design incremental loading; handle late data, replay, and partial failure |
| Behavioral/project reasoning | 8 min | Explain decisions, debugging evidence, learning, and collaboration |

Score correctness, clarity, evidence, and tradeoff awareness. Use the final 45-minute repair block on the weakest answers.

[Back to top](#top)

## Definition of done

- [x] Seed contracts and exact expected counts/totals are recorded here.
- [ ] Three deliveries produce a unique, typed, reconciled sales fact and traceable quarantine.
- [ ] Corrections, stale versions, conflicts, cancellations, and late arrivals follow the stated rules.
- [ ] Replay preserves business state; controlled failures recover without publishing partial results.
- [ ] Meaningful unit/integration tests and eight SQL exercises are complete.
- [ ] Spark plan observations distinguish measured evidence from scale-related reasoning.
- [ ] Implemented behavior, local limitations, and conceptual extensions are described honestly.
- [ ] A two-minute walkthrough and 45-minute mock interview are complete; weak areas are reviewed.

[Back to top](#top)
