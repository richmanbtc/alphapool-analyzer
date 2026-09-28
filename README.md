Stock analyzer status

- The stock analyzer is under development and unstable. Its behavior and results
  are not yet considered production-ready.

Input schema

- Requires Python 3.12 or later. Rebuild the Docker image after updating dependencies.
- Position reads use a direct psycopg2 connection; the read transaction is
  rolled back and the connection closed after each fetch, including on failure.
- The analyzer reads PostgreSQL directly and does not depend on the alphapool
  Python package. It does not alter input tables or indexes. Prepare the
  `positions` table using the
  [upstream migrations](https://github.com/richmanbtc/alphapool/tree/v0.2.0/migrations).
  For an existing database, check its schema and migration history first: these SQL
  files are not safe to rerun.

BigQuery output

- Both analyzers write `analyzer_positions` and `analyzer_rets` to BigQuery.
  PostgreSQL remains the source of positions; analyzer writes and VACUUM are removed.
- Environment variable names are mapped once in `src/settings.py` (`ENV_NAMES`).
  Call sites use logical names; values are read when needed, not cached at import.
- Set `ALPHAPOOL_ANALYZER_DATASET` to a dataset ID or a project-qualified dataset ID.
  It defaults to `ALPHAPOOL_DATASET`. `GC_PROJECT_ID` optionally selects the client
  project; authentication uses Application Default Credentials.
- Create the dataset first. The runtime identity needs BigQuery Job User on the
  job project and BigQuery Data Editor on the output dataset.
- Tables are created automatically, partitioned by `timestamp` (TIMESTAMP, UTC)
  and clustered by `model_id` (STRING). Positions also contain `symbol` (STRING),
  `position` and `position_diff` (FLOAT64); returns contain `ret` (FLOAT64).
- Each run loads staging tables, then replaces both outputs in one transaction.
  Staging tables are deleted after use and expire after one day as a fallback.
  Each run replaces only its half-open processing interval [start, end).
  Returns retain their history. Positions older than seven days (crypto) or
  56 days (stock), measured from the current execution time, are deleted.
  Historical catch-up does not reinsert expired positions.
  Empty results still clear the interval and advance progress. Use separate output
  datasets for crypto and stock: output rows do not contain a mode column.
- Existing PostgreSQL results are not migrated.

Missing analysis data

- An empty source on the first run skips processing without creating progress.
  Once a processing interval is known, empty or invalid results still advance it.
  Database/query failures propagate without advancing progress.
- Missing or invalid prices omit only returns for models exposed to those symbols
  at the affected timestamps. Other models and timestamps continue normally.
- Zero exposure contributes zero return even when prices are unavailable.
- Missing or expired portfolio references contribute zero; valid components remain.
- Calculated output replaces only the processing interval, so omitted returns
  appear as gaps instead of retaining previously calculated values in that window.

Incremental execution

- `analyzer_progress` stores `mode`, `completed_until` (an exclusive UTC boundary),
  and `updated_at` as append-only history. Each successful interval appends one
  record; resumption uses `MAX(completed_until)` for the mode. Existing latest-only
  records remain valid and need no migration. Tables are created automatically.
  Results and progress commit
  in one BigQuery transaction. A stale checkpoint assertion rejects a writer if
  another run has already advanced the same mode. Run one task per invocation;
  avoid overlapping invocations for the same output dataset.
- With no checkpoint, processing begins at the earliest timestamp in the input
  PostgreSQL `positions` table, even if older analyzer results already exist.
  No input rows means no checkpoint. Existing results outside the processed
  interval remain intact, except for position retention cleanup.
- Each invocation processes exactly one interval, with no catch-up loop.
  Crypto advances at most one day and reprocesses the previous day. Stock advances
  at most seven days and reprocesses the previous seven days. During catch-up this
  bounds the main input range to two or 14 days, plus boundary snapshots/prices.
  The scheduler must run more frequently than the amount of history advanced.
  `src/settings.py` defines per-mode retention, advance, overlap, and frequency.
  Position validity is a separate constant; equal durations are not coupled.
- The end is capped by the latest available completed market timestamp. Crypto
  excludes bars newer than the preceding five-minute bar; stock excludes today's
  daily bar. The latest available bar/day supplies a successor price and is not
  itself marked processed. Different symbols can still have gaps; these produce
  missing returns, not a blocked checkpoint. With no completed market rows, wait.
- Position reads include one preceding snapshot per model. Crypto uses the preceding
  grid point for differences, carries snapshots forward for at most one day, and
  omits times before a model's first snapshot. Exactly 24 hours remains valid;
  the first five-minute sample after that emits zero positions and weights.
  Subsequent expired samples are omitted until the next update. Expired or absent
  portfolio components contribute zero without removing valid components.
  Position differences record the exit to zero and the restart from zero.
  Crypto predecessor reads reach at most one day before the preceding grid
  point (24 hours plus five minutes before the output interval), including
  enough context for expiry differences.
  Stock uses recorded snapshots and its existing session expansion, including
  predecessor sessions.
- Market reads include one successor per symbol beyond the interval, up to the
  completed-bar limit. Stock holidays therefore do not require a fixed lookahead.
- Zero-valued position columns present in the input are retained in incremental
  output, so a column becoming nonzero in another interval does not change whether
  its zero rows are emitted. Symbols absent from all snapshots in an interval
  (including its boundary snapshots) have no position rows in that interval.
- Corrections inside the overlap are picked up automatically. Older corrections
  require an explicit backfill. Input positions and prices must remain available
  for historical recovery. No production checkpoint or output is changed by tests.

Tests

- Run unit tests without a database: `python -m unittest discover -s tests -v`.
- Rebuild the devcontainer to start the Compose app and PostgreSQL services.
  Compose sets test PostgreSQL connection variables.
- Run all tests: `python -m scripts.test_postgres`. Integration tests create a
  unique database, apply the fixture migration, and delete that database even
  when a test fails. The configured role must be able to create databases.
- Outside the devcontainer, run `docker compose run --rm app python -m scripts.test_postgres`,
  or configure standard libpq `PG*` variables for a dedicated test server.
- PostgreSQL is development-only, uses trust authentication without published
  host ports, and stores data in tmpfs. Stopping the service discards its data.
  It is not a production database. Grafana and dashboards are managed separately.
- The migration fixture is copied from alphapool v0.2.0
  (`migrations/0001_initial.sql`). Update it when the input schema changes.
  Fixtures are inserted with SQL; these tests do not exercise the upstream
  Python client or its submission API.
- Integration tests cover real JSONB reads, timestamp filtering, committed-data
  visibility, connection cleanup on success and failure, and both analyzers with
  missing XRP prices. Market data and BigQuery output remain mocked.
- CI runs both suites using the same runner with a PostgreSQL service container.

Analyzer structure

- `src/main.py` and `src/main_stock.py` select the mode and start the shared runner.
- `src/runner.py` coordinates one job. `run_job` accepts an explicit execution time
  and reader/writer functions, so tests do not need environment or clock patches.
- `src/market.py` separates market fetching from pure crypto and stock return
  calculations. The calculation functions accept frames and leave inputs unchanged.
- `src/analysis.py` prepares model positions, fills stock session timestamps,
  builds the equal-weighted portfolio, and converts results into output DataFrames.
- `tests/test_analysis.py` checks expected returns, stock split adjustments,
  session filling, position differences, update windows, and entry-point modes.

- Output DataFrames are passed directly to the BigQuery loader without converting
  to Python row dictionaries. Loads are not split into batches.
- Return calculations align one market-return series at a time to position
  timestamps instead of joining all market columns onto every model row.

Performance benchmark

Run separately from tests; there are no performance pass/fail thresholds:

```sh
python -m scripts.benchmark --mode crypto
python -m scripts.benchmark --mode stock
mkdir -p benchmark-results
python -m scripts.benchmark --mode crypto --models 24 --symbols 24 --repeats 3 --output benchmark-results/benchmark-crypto.json
```

- Requires the pinned dependencies and the test PostgreSQL service (started by
  the devcontainer). This runner uses Linux peak-RSS units.
- Crypto defaults to 24 base models plus 3 non-nested portfolios, 24 symbols,
  and 8 calendar days of positions at 5-minute intervals (2,305 timestamps,
  including both endpoints).
- Stock defaults to 256 models, 32 symbols, and 57 calendar days with one position
  per weekday (41 timestamps for the fixed scenario). Exchange holidays are not
  modeled. The analyzer adds stock session timestamps and its equal-weighted
  portfolio normally.
- Measurement version 3 measures one bounded catch-up interval: two days for
  crypto and 14 days for stock, including overlap. The longer input fixture
  verifies that the database reader bounds its result. Reports from older
  versions cover different workloads and are not directly comparable.
- Each base model holds up to 3 symbols. A fixed seed supplies changing positions,
  a flat model, periodic zero positions, and occasional missing prices.
- Setup creates a unique scratch database on the test PostgreSQL service, applies the pinned migration,
  inserts JSONB input, and generates market Parquet data. Setup is not timed.
- One warm-up and each measured repetition run in fresh child processes. The DB
  remains running, so these are warm-cache measurements, not cold-start DB tests.
- The measured pipeline uses the real job runner and PostgreSQL reader. Market
  acquisition reads the prepared local Parquet file and executes the normal
  market-return calculations. BigQuery operations are offline, but its installed
  SDK's Parquet conversion runs on real output frames using temporary files.
  No BigQuery credentials, network calls, or server-side SQL are involved.
- `pipeline_seconds` includes DB reading through serialization, excluding output checks.
  `process_seconds` also includes interpreter startup/imports and process exit.
  `peak_rss_mib` is the analyzer child process's lifetime peak RSS, including
  native-library allocations and startup; it excludes the PostgreSQL server.
- Each run reports stage times, output counts/sums/fingerprints, and serialized
  sizes. Outputs must match the warm-up, returns must exist, values must be finite,
  and the flat model must have zero returns. These checks detect some incomplete
  runs but do not replace the expected-value tests.
- Reports contain only synthetic scenario parameters and measurements. Scratch databases
  and market fixtures are removed afterwards. Use `--output` to retain a
  JSON report; no report is saved by default.

Benchmark memory details

- Each measured stage now records starting/ending RSS, sampled peak RSS, and
  the process lifetime high-water mark before and after the stage. Sampling is
  best-effort every 5 ms; Python scheduling can miss short peaks. The lifetime
  high-water mark remains the process-wide peak, not an independent stage peak.
  RSS values include data retained from earlier stages and are not additive.
- The sampler adds overhead. Compare runs using the same instrumentation; older
  timing reports without sampling are not directly comparable.
- Position and return outputs concatenate per-column frames for simplicity.
  Numeric values remain float64 and timestamps remain UTC.

- Measurement version 2 separates `parquet_positions` / `parquet_returns` from
  `validate_positions` / `validate_returns`. Validation runs after the job ends;
  it retains references to the output frames without copying them.
- `pipeline_peak_rss_mib` snapshots the process lifetime high-water mark at job
  completion, before validation. Use this for the analyzer workload. It includes
  startup and SDK conversion, but still excludes real BigQuery networking and
  server-side operations. It is not total Cloud Run container memory.
- `validation_seconds` measures the subsequent correctness checks.
  `peak_rss_mib` and `process_seconds` include validation for backward visibility.
  Older reports counted validation inside `pipeline_seconds`; compare reports
  with the same measurement version.

Continuous integration and image publishing

- A single workflow runs unit tests, PostgreSQL integration tests, and a Docker
  build check on every branch or tag push.
- Tags starting with `v` publish an image only after both test and build jobs
  pass. The image tag follows the pushed tag name.
- Images record the source commit in the OCI label
  `org.opencontainers.image.revision`; Git history is excluded from the build.
- Workflows do not deploy applications or run performance benchmarks.

Benchmark result files

- Store raw JSON reports in `benchmark-results/`. This directory is excluded
  from version control and Docker builds; reports are disposable local artifacts.
- Keep benchmark code and reproduction instructions in the repository. When a
  comparison is worth retaining, document its execution conditions and key
  measurements rather than committing raw reports.

Malformed input and staging cleanup

- Invalid model identifiers and missing timestamps are omitted. Non-object
  position or weight payloads mark the snapshot as missing, preventing crypto
  positions from carrying forward through an invalid update.
- Staging cleanup failures emit warnings and do not change the save outcome.
  Cleanup continues for remaining tables; the expiration remains one day.

Market reads and completion logs

- Market queries select only the columns required for the selected analyzer.
- After a successful save, the completion log records the mode, UTC interval
  boundaries (exclusive end), and position and return row counts.
