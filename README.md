# Pack Data Challenge — Diego Pettorossi

> **CEO question:** "Are our 'Top Tier' mentors actually delivering better retention than the standard ones? And is the booking system reliable?"

---

> **macOS / Linux:** use `python3` if `python` is not on your PATH. `make` is available by default on macOS.
> **Windows:** all commands work with plain `python`. For the `make` targets, use Git Bash + `choco install make`, or call the commands directly.

## Quick Start

```bash
# macOS / Linux
make install
make pipeline          # ingest + transform + analysis
make test              # 42 pytest tests
python -m streamlit run app.py   # optional: 4-tab interactive UI
```

```bash
# Windows (no make)
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt

python run_pipeline.py
python -m pytest tests/
python -m streamlit run app.py
```

### Pipeline Modes

The pipeline supports modular execution for development and debugging:

```bash
# Full pipeline (default)
python run_pipeline.py

# Ingest only — load raw data, skip transform/analysis
python run_pipeline.py --mode ingest-only

# Transform only — run dbt + tests, skip ingest (useful after config changes)
python run_pipeline.py --mode transform-only

# Analysis only — run CEO query on existing warehouse (instant feedback)
python run_pipeline.py --mode analysis-only
```

**Use cases:**
- `ingest-only`: Debugging source data issues without rebuilding models
- `transform-only`: Testing schema changes without waiting for ingest
- `analysis-only`: Iterating on the CEO query (tier groupings, window) without re-processing

To change any parameter — tier groups, rebooking window, DQ thresholds, environment — edit **`config.yaml`**. No Python or SQL needs to be touched.

### Optional: run via dbt

```bash
pip install dbt-duckdb          # once

# 1. Load raw tables (ingest layer — unchanged)
python src/ingest.py

# 2. Build staged + mart models with full lineage
cd dbt
dbt snapshot                     # SCD2 for dim_mentors (snap_mentors)
dbt run                          # build staging views + incremental mart tables
dbt test                         # run all schema + relationship tests
dbt docs generate && dbt docs serve   # browse lineage graph in browser
```

---

## Project Structure

```
solution/
├── data/
│   ├── booking_events.json      ← Event stream from 3rd-party booking tool
│   ├── mentor_tiers.csv         ← Manual Ops sheet (mentor tiers & rates)
│   └── users_db_export.csv      ← Postgres DB export (users)
├── src/
│   ├── ingest.py                ← Idempotent raw data loading + pre-flight checks
│   ├── validate.py              ← Pre-flight file validators + 3 post-transform DQ checks
│   └── utils.py                 ← StepTimer (soft + hard-kill timeout), clean_history()
├── dbt/                         ← ★ dbt transformation layer (dbt-duckdb)
│   ├── dbt_project.yml          ← Project config + vars (default_duration_minutes, tier groups)
│   ├── profiles.yml             ← dev + prod both target warehouse.duckdb locally
│   ├── analyses/
│   │   ├── analysis_rebooking.sql     ← Parametric rebooking rate query (CEO analysis)
│   │   ├── booking_reliability.sql    ← Booking outcome rates by tier
│   │   └── booking_intent_rate.sql    ← Intent-to-rebook rate: users who booked within 30d AND eventually attended
│   ├── snapshots/
│   │   └── snap_mentors.sql    ← dbt SCD2 snapshot (tier-change detection)
│   └── models/
│       ├── sources.yml          ← Source declarations + schema tests on raw tables
│       ├── staging/
│       │   ├── stg_users.sql    ← Deduped + cast users (view)
│       │   ├── stg_mentors.sql  ← Cleaned mentors (view)
│       │   ├── stg_session_pairs.sql  ← LEAD-bounded pairing (view)
│       │   └── schema.yml       ← unique / not_null / accepted_values tests
│       └── marts/
│           ├── dim_users.sql    ← Incremental dim, SHA-256 PK
│           ├── dim_mentors.sql  ← Table-materialised dim reading snap_mentors
│           ├── fct_sessions.sql ← Incremental fact, SHA-256 PK
│           └── schema.yml       ← unique / not_null / relationships tests
├── tests/
│   ├── test_pipeline.py         ← 42 tests (34 unit + 8 integration)
│   └── fixtures/                ← seed CSVs/JSON used by integration tests
├── .github/
│   └── workflows/ci.yml         ← pytest matrix (3.11+3.12) + dbt compile check
├── logs/                        ← JSON run logs (logs/YYYY/MM/YYYYMMDD.json)
├── Makefile                     ← make install / test / pipeline / dbt-build / dbt-docs (macOS/Linux)
├── config.yaml                  ← ★ All defaults — edit this, not code
├── config.py                    ← PipelineConfig dataclass + YAML loader
├── run_pipeline.py              ← End-to-end orchestrator + run logging
├── app.py                       ← Streamlit UI: Pipeline / Rebooking Rate / Booking Reliability / Run History
├── warehouse.duckdb             ← Generated warehouse
└── README.md
```

---

## The CEO Answer

### Rebooking Rate by Mentor Tier

**Definition:** % of users who completed a second session within 30 days of their first, attributed to the tier of the mentor in that first session.

| Mentor Tier | Users | Rebooked (30d) | Rebooking Rate |
|---|---|---|---|
| **Gold** | 42 | 26 | **61.9%** |
| Silver/Bronze | 90 | 22 | **24.4%** |

Gold-tier mentors drive a **2.5× higher rebooking rate**. A two-proportion Fisher's exact test yields **p < 0.001** — the difference is statistically significant at this sample size.

**Caveats:**
- With 42 Gold users the result is directional, not conclusive.
- Selection bias is likely: Gold mentors may attract more motivated users, or companies paying for Gold may have stronger learning cultures.
- A causal conclusion requires a longer window (90+ days), company-level controls, or ideally randomised tier assignment.

---

### Alternative interpretation: intent to rebook vs. actual session

The question can also be read as **"What percentage of users demonstrated intent to rebook within 30 days *and* showed up?"** — restricting the numerator to users who explicitly scheduled through the booking system (`fct_bookings`) and then attended, rather than counting any second session that happened to occur via `fct_sessions`.

The distinction matters because `fct_sessions` captures any session that started, regardless of how it was arranged. `fct_bookings` captures only the booking-request → confirmed → attended lifecycle, making intent explicit and measurable. The 30-day window gates only the booking *request*; the session itself may occur after day 30.

**Query source:** `dbt/analyses/booking_intent_rate.sql` — `fct_bookings` joined to first-session tier via `fct_sessions` + `dim_mentors`.

| Mentor Tier | Users | Showed intent (booked ≤30d) | Intent + Attended | Intent rate | Intent→Attended rate |
|---|---|---|---|---|---|
| **Gold** | 42 | 26 | 26 | **61.9%** | **100%** |
| Silver/Bronze | 90 | 25 | 22 | **27.8%** | **88.0%** |

**What this reveals beyond the headline numbers:**

- **Gold headline is unchanged (61.9%)** — every Gold user who explicitly rebooked also attended. There were zero cancellations and zero no-shows among Gold rebookers, meaning the commitment rate (intent → attendance) is 100%.
- **Silver/Bronze intent rate is 27.8%**, 3.4 pp higher than the 24.4% rate from `fct_sessions`. Three Silver/Bronze users scheduled a rebooking within 30 days but cancelled before attending. The booking-system lens exposes this leakage; the session-only lens silently absorbs it.
- **Commitment gap:** of the 25 Silver/Bronze users who expressed intent, 3 (12%) did not follow through. Among Gold rebookers the drop-off is 0%.

**Interpretation:** In this dataset the two metrics converge on the same headline (61.9% vs 24.4%), which means there is no material population of users who had an informal second session without going through the booking flow. However, the `fct_bookings` lens reveals a qualitative difference: Gold users who commit to a return session follow through unconditionally, while a small fraction of Silver/Bronze users cancel. Whether that reflects mentor quality, user motivation, or scheduling friction cannot be determined from this data alone.

---

## Architectural Decisions

### ADR-1: DuckDB over SQLite

DuckDB is columnar and analytical — the right mental model for this workload. It mirrors production warehouses (Snowflake, BigQuery), supports window functions and JSON natively, and costs one `pip install`. SQLite is OLTP-oriented and a poor fit for aggregations and window functions.

### ADR-2: Kimball star schema

A star schema gives each metric a single authoritative home. `fct_sessions` owns session logic; `dim_mentors` owns tier data. The next CEO question requires a new query, not changes to existing models. Each table has a `PRIMARY KEY` constraint and a post-transform DQ check (`validate.py`).

### ADR-3: SHA-256 surrogate keys and explicit schemas

All three tables use **explicit `CREATE TABLE IF NOT EXISTS` schemas** with `PRIMARY KEY` constraints. Surrogate keys use **SHA-256** (not MD5):

| Table | Hash key formula |
|---|---|
| `dim_users` | `SHA256(user_id \| company_id \| signup_date)` |
| `dim_mentors` | `SHA256(mentor_id \| tier \| valid_from)` |
| `fct_sessions` | `SHA256(user_id \| mentor_id \| started_at)` |

SHA-256 is the Data Vault 2.0 standard. MD5's 128-bit output has a collision probability ~10⁻¹⁵ at 10M rows; SHA-256's 256-bit output is negligible at any realistic scale. DuckDB's `SHA256()` function is used directly — no external library needed.

### ADR-4: First-session tier attribution

Rebooking rate is attributed to the tier of the user's *first* mentor. The CEO question is about first impressions and the decision to return. Limitation: users who rebook with a different tier are not tracked across tiers — they count only under their first mentor's tier group.

### ADR-5: Missing `session_ended` defaults to +N minutes

Sessions without a `session_ended` event (browser crash scenario) receive a configurable default duration (30 min). The `is_duration_estimated` boolean flag preserves auditability — the field can be re-scored later with a better estimate (e.g. average duration for that mentor) without touching the pairing logic.

### ADR-6: LEAD-bounded session matching

When `session_ended` is missing, naïve `ROW_NUMBER` matching shifts all subsequent pairings, producing phantom multi-day sessions. `LEAD()` captures the timestamp of the *next* `session_started` for the same `(user_id, mentor_id)` pair and uses it as an upper bound for end-event matching. A missing end event is isolated to exactly one session and does not cascade.

### ADR-7: BIGINT for user_id

`user_id` is `BIGINT` (64-bit) throughout: CSV → Polars `Int64` → DuckDB `BIGINT`. `INTEGER` (32-bit) would silently truncate IDs above ~2.1B. The type is identical at every layer to eliminate conversion surprises.

### ADR-8: CAST() not TRY_CAST()

Data is clean after ingestion. `CAST()` raises immediately on bad values; `TRY_CAST()` silently produces NULLs. A NULL `user_id` in `fct_sessions` would corrupt rebooking metrics without triggering the 5% orphan threshold. Hard failures are preferable to silent corruption.

### ADR-9: Parametric SQL templates

`analysis_rebooking.sql` uses `{PLACEHOLDER}` substitution. The template is valid-looking SQL; all values come from validated `PipelineConfig` methods with quote escaping. Analysis logic stays in SQL; parameterisation logic stays in Python and is independently testable.

### ADR-10: Polars over Pandas

Vectorised Rust execution, Arrow-native zero-copy integration with DuckDB (`.pl()`), and explicit null semantics that raise on mixed-type operations instead of silently coercing. No library in the project requires pandas.

### ADR-11: Hybrid ETL/ELT

| Layer | Pattern | Rationale |
|---|---|---|
| `ingest.py` | ETL (Python/Polars) | Type coercion, dedup, whitespace trimming are awkward in SQL |
| `dbt/models/*.sql` | ELT (DuckDB SQL) | Window functions, CTEs, analytical logic belong in SQL after landing |

### ADR-12: Pre-flight validation before dimension replace

`ingest_mentors()` validates required columns and non-null tiers *before* `DROP TABLE IF EXISTS`. A bad CSV would otherwise silently wipe the dimension and corrupt all downstream joins. DQ checks run post-transform — too late to preserve the previous state.

### ADR-13: CPython-specific two-tier timeout

`StepTimer` uses a two-tier kill strategy:

1. **Soft kill** — injects `PipelineTimeoutError` via `PyThreadState_SetAsyncExc` (CPython-specific, Windows-safe, fires at the next Python bytecode boundary).
2. **Hard kill** — if the soft kill does not resolve within `GRACE_SECONDS=10` (e.g. blocked inside a C-level DuckDB call), `os._exit(1)` forcibly terminates the process.

The hard kill bypasses normal cleanup. It is logged. Not suitable for a multi-threaded server, but correct for a single-worker local pipeline.

---

### ADR-14: Append-only warehouse pattern

All production tables use `CREATE TABLE IF NOT EXISTS` + `INSERT WHERE NOT EXISTS` guarded by the SHA-256 hash key. `CREATE OR REPLACE TABLE` was removed. Rationale:

- Re-running the pipeline never destroys data.
- Multiple runs on the same day are idempotent (same hash → skip).
- Historical snapshots accumulate naturally; SCD2 `valid_to` tracks attribute changes.
- `utils.py clean_history()` provides an explicit reset when needed (`python src/utils.py`).

---

### ADR-15: SCD2 for dimension tables

Both dimension tables track attribute history:

| Field | dim_users | dim_mentors |
|---|---|---|
| `valid_from` | `signup_date` (natural date) | `CURRENT_DATE` of first insert |
| `valid_to` | `NULL` (no attribute changes in this dataset) | Set when tier changes |
| `is_current` | Derived: `valid_to IS NULL` | Derived: `valid_to IS NULL` |

`is_current` is never stored — computed at query time. `analysis_rebooking.sql` always joins `dim_mentors` with `AND m.valid_to IS NULL` to use only the current tier.

---

### ADR-16: dbt as the recommended transformation layer

The `dbt/` directory provides a **dbt-duckdb** project for all transformation logic. It adds:

- **Lineage graph** — `dbt docs generate` produces a full DAG from source files to mart tables.
- **Column-level tests** — `dbt test` runs `unique`, `not_null`, `relationships`, and `accepted_values` on every column without custom code.
- **dbt snapshot for SCD2** — `snap_mentors` handles tier-change detection, `valid_to` closing, and new-record insertion automatically. No manual UPDATE+INSERT.
- **Parameterised via dbt vars** — `default_duration_minutes` is a dbt variable, overridable at runtime without touching Python.
- **Incremental materialisation** — `dim_users` and `fct_sessions` use `materialized='incremental'` with `unique_key` and `merge` strategy. `dim_mentors` uses `materialized='table'` with a `QUALIFY ROW_NUMBER()` dedup guard to prevent duplicate tier rows from accumulating across pipeline runs.

---

### ADR-17: Environment isolation (dev / staging / prod)

`config.yaml` has an `environment` field. The default `db_path` is derived from the environment unless explicitly overridden:

| Environment | Default db_path |
|---|---|
| `dev` | `warehouse_dev.duckdb` |
| `staging` | `warehouse_staging.duckdb` |
| `prod` | `warehouse.duckdb` |

dbt profiles mirror `config.yaml`: both `dev` and `prod` targets point to `warehouse.duckdb` locally. On a real deployment, point `profiles.yml` at separate Snowflake schemas or MotherDuck databases per environment.

---

### ADR-18: GitHub Actions CI/CD

`.github/workflows/ci.yml` runs on every push and pull request:

1. **pytest job** — Matrix across Python 3.11 and 3.12; installs both requirements files; runs all 42 tests (34 unit + 8 integration) with `--tb=short`.
2. **dbt-compile job** (runs after pytest) — `dbt compile` validates that all SQL is parseable and all `ref()` / `source()` references resolve, including `analyses/analysis_rebooking.sql`, catching schema regressions before merge.

---

## Data Quality Issues Found

| Source | Issue | Resolution |
|---|---|---|
| `users_db_export.csv` | 5 duplicate user rows (app retry bug) | Deduplicated at ingestion — keep first occurrence |
| `booking_events.json` | `user_id` is sometimes int, sometimes string | Normalised to BIGINT before any join |
| `booking_events.json` | Missing `session_ended` events (browser crash) | Default +30 min, flagged with `is_duration_estimated` |
| `mentor_tiers.csv` | Potential whitespace in `mentor_id` | `TRIM()` applied at ingestion |

---

## Data Quality Checks

Automated checks run after every transformation in two layers:

**Layer 1 — dbt test** (`dbt/models/marts/schema.yml`): `not_null`, `unique`, and `relationships` assertions run via `dbt test` inside `run_pipeline.py`. These cover referential integrity (orphan users, unmatched mentors) and primary key uniqueness. A dbt test failure halts the pipeline before Python checks run.

**Layer 2 — Python checks** (`src/validate.py`): business-logic checks that dbt cannot express declaratively. Checks 1, 5, 6 halt the pipeline on failure. Check 4 warns only.

| # | Check | Where | Threshold | Result |
|---|---|---|---|---|
| — | Orphan sessions (no matching user) | dbt `relationships` | 0 | ✅ PASS |
| — | Unmatched mentor_ids in sessions | dbt `relationships` | 0 | ✅ PASS |
| 1 | No negative session durations | validate.py | 0 | ✅ PASS |
| 4 | Session duration > 4 hours | validate.py | Warning only | ✅ PASS (0 outliers) |
| 6 | Configured tier names exist in dim_mentors | validate.py | 0 missing | ✅ PASS |

---

## Tracking Plan (Example)

### Naming conventions

| Rule | Example |
|---|---|
| Event names: `object_action` snake_case | `booking_requested`, `session_ended` |
| Property names: snake_case | `mentor_id`, `is_duration_estimated` |
| IDs: always strings in the payload (cast to BIGINT at ingestion) | `"user_id": "42"` |
| Timestamps: ISO-8601 UTC | `"timestamp": "2026-01-15T10:00:00Z"` |
| Booleans: explicit `true`/`false` | `"had_session_started": true` |

### Event catalogue

#### Booking lifecycle (currently in `booking_events.json`)

| Event | Trigger | Required properties | Optional properties |
|---|---|---|---|
| `booking_requested` | User submits booking form | `event_id`, `user_id`, `mentor_id`, `timestamp` | `requested_duration_minutes` |
| `booking_confirmed` | Mentor accepts | `event_id`, `user_id`, `mentor_id`, `timestamp` | `confirmed_start_time` |
| `booking_cancelled` | Either party cancels | `event_id`, `user_id`, `mentor_id`, `timestamp` | `cancelled_by` (`user`\|`mentor`), `cancellation_reason` |
| `session_started` | Session call begins | `event_id`, `user_id`, `mentor_id`, `timestamp` | — |
| `session_ended` | Session call ends | `event_id`, `user_id`, `mentor_id`, `timestamp` | `duration_seconds` |

**Gap identified in current data:** `booking_cancelled` does not include `cancelled_by`. Without it, the pipeline cannot distinguish user-initiated vs mentor-initiated cancellations — a key churn signal.

#### User lifecycle (currently missing — should be added)

| Event | Trigger | Required properties | Optional properties |
|---|---|---|---|
| `user_signed_up` | Account creation completes | `user_id`, `company_id`, `timestamp` | `signup_source` (`organic`\|`invite`\|`sso`) |
| `user_activated` | First session completed | `user_id`, `timestamp` | — |
| `user_churned` | Status set to `churned` in Postgres | `user_id`, `timestamp` | `churn_reason` |

#### Mentor engagement (currently missing)

| Event | Trigger | Required properties | Optional properties |
|---|---|---|---|
| `mentor_profile_viewed` | User views mentor profile | `user_id`, `mentor_id`, `timestamp` | `viewed_from` (`search`\|`recommended`\|`direct`) |
| `session_reviewed` | User submits post-session rating | `user_id`, `mentor_id`, `session_id`, `timestamp`, `rating` (1–5) | `review_text` |

### Data destinations

| Source | Transport | Raw landing | Notes |
|---|---|---|---|
| Web/mobile app events | PostHog JS SDK → `booking_events` stream | `raw_events` table | PostHog HogQL can query directly; Fivetran syncs to warehouse |
| Postgres (users, status) | Fivetran CDC connector | `raw_users` table | `updated_at` column required for SCD2 |
| Ops sheet (mentor tiers) | Fivetran Google Sheets connector | `raw_mentors` table | Sync weekly (low change frequency) |

### Required Postgres columns to add

For correct SCD2 and CDC, the Postgres `users` table must expose:
```sql
ALTER TABLE users ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();
CREATE TRIGGER set_updated_at BEFORE UPDATE ON users
  FOR EACH ROW EXECUTE FUNCTION trigger_set_timestamp();
```
Without `updated_at`, Fivetran falls back to full-table sync and `valid_from` in `dim_users` reflects pipeline run time, not the business event time (see Known Limitations §1).

### Instrumentation checklist (for engineering handoff)

- [ ] Add `cancelled_by` and `cancellation_reason` to `booking_cancelled` events
- [ ] Emit `user_signed_up` from the registration flow
- [ ] Emit `session_reviewed` after post-session NPS prompt
- [ ] Ensure all events include `event_id` (UUID v4) for idempotent ingestion
- [ ] Add `updated_at` trigger to Postgres `users` table
- [ ] Standardise `mentor_id` format in the booking tool to match the Ops sheet (currently requires `TRIM()` at ingestion)

---

## Production Roadmap

**Guiding principle: trust the data before you trust the metric.**

**Implemented in this solution.** The following items from the typical production roadmap are already done:

- ✅ dbt project with staging, mart models, `dbt snapshot` for SCD2, and `dbt test` assertions.
- ✅ SHA-256 surrogate keys on all three tables (Data Vault 2.0 standard).
- ✅ Append-only warehouse — re-runs never destroy data; `clean_history()` for explicit reset.
- ✅ SCD2 on both dimensions with `valid_to IS NULL` = current record.
- ✅ Environment isolation: `dev` / `staging` / `prod` db paths from `config.yaml`.
- ✅ GitHub Actions CI: pytest matrix (Python 3.11 + 3.12) + dbt compile check on every PR.
- ✅ 42 tests: 34 unit tests (monkeypatched file I/O) + 8 integration tests that run ingest → SQL → analysis against real fixture data.
- ✅ Wilson score 95% CI printed alongside every tier's rebooking rate at runtime.
- ✅ `analysis_rebooking.sql` moved into `dbt/analyses/` — compiled by `dbt compile`, visible in the lineage graph.
- ✅ Two-tier timeout: soft bytecode injection + hard `os._exit(1)` after 10s grace.

**Next: move to managed infrastructure.**

**Week 1 — Reliable ingestion at scale.** Fivetran (or Airbyte) syncs Postgres → Snowflake raw layer — just reliable landing with schema drift detection. Booking events go via webhook-to-S3; mentor tiers via a Fivetran Google Sheets connector.

**Week 2 — dbt in production.** The `dbt/` project in this repo is production-ready. Point `profiles.yml` at Snowflake (or MotherDuck) and run `dbt run --target prod`. Add dbt Cloud for scheduled runs with Slack alerting on failure. Replace the `INSERT WHERE NOT EXISTS` dedup logic with watermark-based incremental loads for O(n) instead of full-table scans.

**Week 3 — Visible metrics.** Metabase or Lightdash dashboard with daily refresh. Lightdash integrates directly with dbt (version-controlled dashboards); Metabase is easier for non-technical users.

**What not to build yet.** Spark, Kafka, streaming. With ~150 users and 15 mentors, complexity is the failure mode. Boring, reliable tools first.

---

## Known Limitations

The following items are understood and documented; they are not implemented in this challenge solution.

**1. SCD2 `valid_from` is the pipeline run date, not the source effective date.**  
`dim_users.valid_from` uses `CURRENT_DATE` of first insert rather than the `signup_date` sourced from Postgres. The Postgres export includes `signup_date` and it is stored in the dimension, but the SCD2 open/close timestamps track pipeline processing time, not business time. In production, the correct fix is to source a CDC stream (Debezium or Fivetran) that emits the exact row-level `updated_at` from Postgres and use that as `valid_from`.

**2. DuckDB single-writer concurrency.**  
DuckDB allows only one writer at a time. The Streamlit app launches the pipeline as a subprocess and holds no persistent connection during normal operation, so read interactions are safe. However, triggering a pipeline run while a previous run is still in progress, or clicking "Clear warehouse" while a run executes, will raise a `databaseError`. In production, replace DuckDB with MotherDuck (serverless, multiple concurrent connections) or a proper warehouse (Snowflake, BigQuery, Redshift) where read/write concurrency is handled at the engine level.

**3. No workflow orchestration.**  
The pipeline is run manually (`python run_pipeline.py`) or via a scheduled GitHub Actions cron job. There is no Airflow, Prefect, or Dagster DAG. Dependency ordering, retry policies, SLA alerting, and backfill support are missing. In production, wrap each pipeline step as a dbt node or a Prefect task so failures are observable, retriable, and auditable.

---

## Honest Critical Assessment

This solution implements most modern data stack patterns at local scale.

**Strengths**
- Clean separation: config, ingestion, transformation, analysis are each independently changeable.
- LEAD-bounded session matching correctly handles missing `session_ended` events, covered by three targeted tests.
- Pre-flight validation catches bad source files before any write occurs, protecting existing warehouse state.
- SHA-256 surrogate keys on all three tables with `PRIMARY KEY` constraints at the DB layer.
- SCD2 on both dimensions with `valid_to IS NULL` = current; dbt snapshot handles tier-change detection automatically.
- Append-only warehouse: re-running never destroys data; `clean_history()` provides explicit reset.
- Mode flags (`--mode ingest-only`, `transform-only`, `analysis-only`) enable modular development and debugging.

**Remaining gaps**

*Incremental load uses full-table scan.* `INSERT WHERE NOT EXISTS` checks the entire table on every run. At scale this needs a watermark (`WHERE inserted_at > last_run_max`) to avoid O(n²) behaviour.

*SCD2 `valid_from` is pipeline run date, not source effective date.* If a tier change is backdated in the source file, `valid_from` reflects when the pipeline ran, not when the change was effective. Correct CDC requires a source-provided effective date.

*Single-writer DuckDB.* The Streamlit app launches the pipeline as a subprocess and holds no persistent connection during normal operation. However, triggering a concurrent run or using “Clear warehouse” while a pipeline executes will raise a `databaseError`. In production, replace DuckDB with MotherDuck or a proper warehouse where concurrency is managed at the engine level.

*Sample size limits conclusions.* 42 Gold-tier users produces a real p < 0.001, but the confidence interval on the 61.9% rate is wide (~47–75%). The result is directional, not conclusive.

*The rebooking metric is a single all-time cohort.* A cohort-by-month view would reveal seasonality or tier pricing changes that the current single-window query silently absorbs.

---

## Migrating to AWS/Snowflake: First Principles Checklist

**Context:** This solution is local-first (DuckDB, file-based config, manual execution).

### 1. Data Ingestion — From Local Files to Managed Streams

**Current state:** `data/*.csv` and `data/*.json` on disk. `src/ingest.py` reads with Polars, writes to DuckDB.

**AWS/Snowflake migration:**

| Component | Local | AWS/Snowflake |
|---|---|---|
| **Users export** | `data/users_db_export.csv` | Fivetran or AWS DMS: Postgres → Snowflake `raw.users` table (CDC) |
| **Booking events** | `data/booking_events.json` | API webhook → AWS Lambda → S3 `s3://pack-events/YYYY/MM/DD/*.json` → Snowpipe auto-ingest |
| **Mentor tiers** | `data/mentor_tiers.csv` | Google Sheets → Fivetran → Snowflake `raw.mentor_tiers` (hourly sync) |

**Why this matters:** Local files don't auto-update. In production, data must arrive continuously without manual intervention. Fivetran handles schema drift (new columns), retry logic, and incremental sync. Snowpipe handles micro-batching of S3 events.

**Code changes required:**
- Delete `src/ingest.py` entirely (Fivetran/Snowpipe replace it)
- Update `dbt/models/sources.yml` to point at Snowflake `raw.*` tables instead of DuckDB
- Add Snowflake credentials to dbt `profiles.yml` (use environment variables, never commit secrets)

---

### 2. Warehouse — From Single-File DuckDB to Multi-User Snowflake

**Current state:** `warehouse.duckdb` file. Single writer. No RBAC. No audit log.

**AWS/Snowflake migration:**

| Concern | DuckDB (local) | Snowflake |
|---|---|---|
| **Concurrency** | Single writer; `app.py` and `run_pipeline.py` conflict | Unlimited concurrent reads + writes |
| **Permissions** | None (anyone with file access can `DROP TABLE`) | Role-based: `TRANSFORMER` role can only write to `analytics.*`; `ANALYST` role read-only |
| **Compute isolation** | Shares CPU/RAM with laptop | Separate warehouses for `ETL_WH` (large), `REPORTING_WH` (small) |
| **Audit** | No log of who queried what | `QUERY_HISTORY` table tracks every query with user, timestamp, cost |
| **Disaster recovery** | Lost if laptop dies | Time Travel (90 days), Fail-safe (7 days after Time Travel) |

**Code changes required:**
- Replace `DB_PATH = "warehouse.duckdb"` with `snowflake.connector.connect()` using `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` env vars
- Update dbt `profiles.yml`:
  ```yaml
  pack_data:
    target: prod
    outputs:
      prod:
        type: snowflake
        account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
        user: "{{ env_var('SNOWFLAKE_USER') }}"
        password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
        role: TRANSFORMER
        warehouse: ETL_WH
        database: ANALYTICS
        schema: MAIN
  ```
- Grant permissions:
  ```sql
  CREATE ROLE TRANSFORMER;
  GRANT CREATE TABLE ON SCHEMA analytics.main TO ROLE TRANSFORMER;
  GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE TRANSFORMER;
  
  CREATE ROLE ANALYST;
  GRANT SELECT ON ALL TABLES IN SCHEMA analytics.main TO ROLE ANALYST;
  ```

---

### 3. Orchestration — From Manual `python run_pipeline.py` to Scheduled DAGs

**Current state:** Run manually or via GitHub Actions cron. No dependency graph. No retry on transient failure.

**AWS/Snowflake migration:**

| Tool | Why | When to use |
|---|---|---|
| **dbt Cloud** | Native dbt scheduler. GUI for non-engineers. Slack alerts on failure. | Small team, dbt-only transforms |
| **Airflow (MWAA)** | Full DAG control. Python sensors. Cross-tool orchestration (dbt + Spark + API calls). | Complex pipelines with non-dbt steps |
| **Prefect Cloud** | Modern Airflow alternative. Better UI. Easier local dev. | Hybrid (some transforms in Python, some in dbt) |
| **Step Functions** | AWS-native. Cheap. JSON-based state machines. | Stateless workflows, no custom logic |

**Code changes required (Airflow example):**
- Create `airflow/dags/pack_pipeline.py`:
  ```python
  from airflow import DAG
  from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
  from airflow.operators.python import PythonOperator
  from datetime import datetime
  
  with DAG(
      "pack_pipeline",
      start_date=datetime(2026, 1, 1),
      schedule="0 2 * * *",  # 2 AM daily
      catchup=False,
  ) as dag:
      # Fivetran syncs run automatically; no ingest task needed
      
      dbt_run = DbtCloudRunJobOperator(
          task_id="dbt_run",
          job_id=12345,  # dbt Cloud job ID
          check_interval=60,
          timeout=1800,
      )
      
      def _run_analysis():
          # Execute analysis_rebooking.sql via Snowflake Python connector
          # Send results to Slack webhook
          pass
      
      analysis = PythonOperator(
          task_id="ceo_analysis",
          python_callable=_run_analysis,
      )
      
      dbt_run >> analysis
  ```
- Delete `run_pipeline.py` or keep as a local dev tool only
- Set up Airflow MWAA environment in AWS (costs ~$300/month minimum)

---

### 4. Environment Management — From Single `warehouse.duckdb` to Dev/Staging/Prod

**Current state:** `config.yaml` has an `environment` field but all envs write to the same DuckDB file locally.

**AWS/Snowflake migration:**

| Environment | Purpose | Snowflake setup |
|---|---|---|
| **Dev** | Engineers test schema changes | Separate schema `ANALYTICS_DEV.MAIN` (cloned weekly from prod) |
| **Staging** | QA + stakeholder preview | Separate database `ANALYTICS_STAGING` (full production pipeline, subset of data) |
| **Prod** | Live CEO dashboard | Database `ANALYTICS`, read-only for analysts, write for `TRANSFORMER` role |

**Code changes required:**
- Update dbt `profiles.yml` to have three targets: `dev`, `staging`, `prod`
- CI/CD pipeline:
  ```yaml
  # .github/workflows/deploy.yml
  - name: Deploy to dev
    if: github.ref == 'refs/heads/main'
    run: dbt run --target dev
  
  - name: Deploy to staging
    if: github.event_name == 'release' && github.event.action == 'prereleased'
    run: dbt run --target staging
  
  - name: Deploy to prod
    if: github.event_name == 'release' && github.event.action == 'published'
    run: dbt run --target prod
  ```
- Snowflake schema cloning for dev refreshes:
  ```sql
  -- Weekly: refresh dev with prod structure, empty data
  CREATE OR REPLACE SCHEMA analytics_dev.main CLONE analytics.main;
  TRUNCATE TABLE analytics_dev.main.fct_sessions;  -- keep dim tables for joins
  ```

---

### 5. Secrets Management — From No Secrets to AWS Secrets Manager

**Current state:** No credentials (local file access only).

**AWS/Snowflake migration:**

| Secret | Local | Production |
|---|---|---|
| Snowflake password | N/A | AWS Secrets Manager `pack/snowflake/transformer_password` |
| dbt Cloud API key | N/A | AWS Secrets Manager `pack/dbt/api_key` |
| Google Sheets API (Fivetran) | N/A | Fivetran vault (managed by Fivetran) |
| Slack webhook (alerts) | N/A | AWS Secrets Manager `pack/slack/webhook_url` |

**Code changes required:**
- Add `boto3` to `requirements.txt`
- Update `config.py` to fetch secrets:
  ```python
  import boto3
  import json
  
  def get_secret(secret_name):
      client = boto3.client('secretsmanager', region_name='us-east-1')
      return json.loads(client.get_secret_value(SecretId=secret_name)['SecretString'])
  
  SNOWFLAKE_PASSWORD = get_secret('pack/snowflake/transformer_password')['password']
  ```
- IAM policy for Lambda/Airflow execution role:
  ```json
  {
    "Effect": "Allow",
    "Action": "secretsmanager:GetSecretValue",
    "Resource": "arn:aws:secretsmanager:us-east-1:123456789:secret:pack/*"
  }
  ```

---

### 6. Observability — From `logs/*.json` to CloudWatch + dbt Artifacts

**Current state:** Local JSON logs in `logs/YYYY/MM/*.json`. No aggregation. No alerts.

**AWS/Snowflake migration:**

| Metric | Local | Production |
|---|---|---|
| **Pipeline runs** | JSON file per run | CloudWatch Logs: one log stream per DAG run |
| **dbt test failures** | Printed to stdout | dbt Cloud UI + Slack alert |
| **Row counts** | Logged to console | Send to CloudWatch Metrics → Grafana dashboard |
| **Query performance** | DuckDB has no profiling | Snowflake `QUERY_HISTORY` → detect slow queries (>5 min) → auto-alert |
| **Cost** | Free (local CPU) | Snowflake credit usage tracked in `WAREHOUSE_METERING_HISTORY` → alert if daily cost >$50 |

**Code changes required:**
- Replace `PipelineRunLog` JSON writes with CloudWatch:
  ```python
  import boto3
  logs = boto3.client('logs')
  logs.put_log_events(
      logGroupName='/pack/pipeline',
      logStreamName=run_id,
      logEvents=[{'timestamp': int(time.time() * 1000), 'message': json.dumps(event)}]
  )
  ```
- dbt Cloud artifacts (compiled SQL, test results) auto-uploaded to dbt Cloud S3 bucket; viewable in UI
- Snowflake cost monitoring query (run daily):
  ```sql
  SELECT
      DATE_TRUNC('day', start_time) AS day,
      warehouse_name,
      SUM(credits_used) AS total_credits
  FROM snowflake.account_usage.warehouse_metering_history
  WHERE start_time >= DATEADD(day, -7, CURRENT_DATE)
  GROUP BY 1, 2
  ORDER BY 3 DESC;
  ```
  If `total_credits > threshold`, send SNS alert → PagerDuty/Slack.

---

### 7. Testing — From 40 Pytest Tests to CI + Data Quality Monitors

**Current state:** 40 pytest tests run in GitHub Actions. All tests use in-memory DuckDB.

**AWS/Snowflake migration:**

| Test type | Local | Production |
|---|---|---|
| **Unit tests** (ingest logic) | pytest with mocked file I/O | Same (keep as-is; fast, no DB needed) |
| **Integration tests** | pytest with in-memory DuckDB | pytest with Snowflake `ANALYTICS_DEV` (cloned schema, real warehouse) |
| **dbt tests** | `dbt test` on DuckDB | `dbt test` on Snowflake dev schema (runs in CI on every PR) |
| **Data quality monitors** | `validate.py` halts pipeline on failure | Great Expectations + Monte Carlo → alerts on anomaly (don't halt production) |

**Code changes required:**
- Update `tests/test_pipeline.py` to connect to Snowflake dev when `CI=true`:
  ```python
  @pytest.fixture
  def con():
      if os.getenv("CI"):
          return snowflake.connector.connect(
              account=os.getenv("SNOWFLAKE_ACCOUNT"),
              user="CI_USER",
              password=os.getenv("SNOWFLAKE_CI_PASSWORD"),
              database="ANALYTICS_DEV",
              schema="MAIN",
          )
      else:
          return duckdb.connect(":memory:")  # local dev stays fast
  ```
- Add Great Expectations for runtime data quality:
  ```yaml
  # great_expectations/expectations/fct_sessions.json
  expectation_suite_name: fct_sessions
  expectations:
    - expectation_type: expect_column_values_to_be_between
      kwargs:
        column: duration_minutes
        min_value: 0
        max_value: 300  # no session >5 hours
    - expectation_type: expect_table_row_count_to_be_between
      kwargs:
        min_value: 150  # freeze if row count drops (data loss signal)
  ```
  Run after dbt; log results to S3; alert on failure.

---

### 8. Cost Optimization — From Free Local Compute to Pay-Per-Query Snowflake

**Current state:** No cost (local laptop CPU).

**AWS/Snowflake migration:**

| Cost driver | Mitigation |
|---|---|
| **Snowflake compute** (warehouse hours) | Use `AUTO_SUSPEND = 60` (idle 1 min → suspend). Use X-Small warehouse for dev/staging. |
| **Snowflake storage** | Enable `DATA_RETENTION_TIME_IN_DAYS = 1` for dev (vs. 90-day default). Cluster on date columns. |
| **Fivetran sync** | Sync only changed rows (Fivetran default). Reduce sync frequency for `mentor_tiers` (static data) to weekly. |
| **dbt Cloud** | Team plan ($100/seat/month). Use Airflow + dbt Core (free) if budget-constrained. |
| **CloudWatch Logs** | Set retention to 7 days (vs. infinite default). Export to S3 for long-term audit ($0.03/GB). |

**Example monthly AWS/Snowflake cost (150 users, 200 sessions/day):**
- Snowflake: ~$200 (X-Small warehouse, 4 hours/day runtime)
- Fivetran: $150 (3 connectors, 10K rows/month each)
- dbt Cloud: $100 (1 developer seat)
- AWS (Lambda, S3, Secrets Manager, CloudWatch): ~$50
- **Total: ~$500/month**

---

### 9. Deployment — From Git Push to Infrastructure-as-Code

**Current state:** Manual `python run_pipeline.py`. GitHub Actions runs tests but doesn't deploy.

**AWS/Snowflake migration:**

**Terraform setup (one-time):**
```hcl
# terraform/main.tf
resource "snowflake_database" "analytics" {
  name = "ANALYTICS"
  data_retention_time_in_days = 90
}

resource "snowflake_warehouse" "etl_wh" {
  name           = "ETL_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_role" "transformer" {
  name = "TRANSFORMER"
}

resource "snowflake_grant_privileges_to_role" "transformer_db" {
  privileges = ["USAGE", "CREATE SCHEMA"]
  role_name  = snowflake_role.transformer.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.analytics.name
  }
}
```

**CI/CD pipeline:**
```yaml
# .github/workflows/deploy.yml
name: Deploy to Snowflake
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: hashicorp/setup-terraform@v2
      
      - name: Terraform plan
        run: terraform plan -out=tfplan
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
      
      - name: Terraform apply
        if: github.ref == 'refs/heads/main'
        run: terraform apply tfplan
      
      - name: dbt run (production)
        run: |
          dbt deps
          dbt run --target prod --profiles-dir .
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
```

---

### 10. Summary: Full Migration Checklist

**Phase 1: Lift-and-shift (2 weeks)**
- [ ] Create Snowflake account + dev/staging/prod databases
- [ ] Update dbt `profiles.yml` to connect to Snowflake
- [ ] Set up Fivetran connectors (Postgres, Google Sheets, S3)
- [ ] Replace `src/ingest.py` with Fivetran/Snowpipe (delete ingest code)
- [ ] Run `dbt run --target dev` manually to verify schema creation
- [ ] Update pytest to use Snowflake dev for integration tests

**Phase 2: Orchestration (1 week)**
- [ ] Set up dbt Cloud or Airflow MWAA
- [ ] Create scheduled job: `dbt snapshot` → `dbt run` → `dbt test` daily at 2 AM UTC
- [ ] Add Slack webhook for failure alerts
- [ ] Retire manual `python run_pipeline.py` execution

**Phase 3: Production-ready (1 week)**
- [ ] Terraform all Snowflake resources (databases, warehouses, roles, grants)
- [ ] Move credentials to AWS Secrets Manager
- [ ] Set up CloudWatch logging for pipeline runs
- [ ] Configure Snowflake cost alerts (SNS topic → Slack)
- [ ] Add Great Expectations for runtime data quality checks
- [ ] Document runbook: how to backfill, how to add a new tier, how to debug a failed run

**Phase 4: Optimization (ongoing)**
- [ ] Replace `INSERT WHERE NOT EXISTS` full-table scans with incremental watermarks (`MAX(updated_at)`)
- [ ] Add clustering on `fct_sessions.started_at` for query performance
- [ ] Benchmark Snowflake warehouse sizing (X-Small vs. Small) and right-size for cost
- [ ] Set up Grafana dashboard for pipeline health (row counts, runtime, test pass rate)

**Total migration effort: ~4 weeks (1 engineer).**

**Key principle:** Migrate in phases. Phase 1 (Snowflake + dbt) delivers immediate value (multi-user access). Airflow and Terraform can wait until Phase 2/3 when manual workflows become a bottleneck.
