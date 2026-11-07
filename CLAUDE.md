# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Databricks (Free Edition) medallion pipeline for an online shop: daily order CSVs + ECB exchange rates (Frankfurter API) → revenue in EUR. Responsibilities are split between two tools, with no duplicated logic:

1. **Lakeflow Spark Declarative Pipelines** in `pipeline/shop-pipeline/` — **bronze ingestion only**: loads raw orders and exchange rates into `workspace.shop.bronze_orders` / `bronze_rates`.
2. **dbt project** in `dbt/` (`dbt/dbt_project.yml`, `dbt/models/`) — all transformation logic (staging → intermediate → marts) on top of the bronze tables (declared as dbt source `shop`). Production marts land in `workspace.prod_shop_marts`.

Business rules (validation, dedup, currency conversion, revenue definitions) live only in dbt.

## Lakeflow pipeline (`pipeline/shop-pipeline/`)

- Code runs inside Databricks only — there is no local execution or test harness. Python files use `from pyspark import pipelines as dp` (`@dp.table`, `@dp.materialized_view`, `@dp.expect_or_drop` / `expect_or_fail`) and rely on the implicit `spark` global. Tables reference each other by bare name (default catalog/schema `workspace.shop` is set on the pipeline).
- `bronze_orders.py`: Auto Loader over `/Volumes/workspace/shop/raw_files/` (streaming, incremental, all columns strings).
- `bronze_rates.py`: materialized view that calls the Frankfurter API on every run (`API_URL` has a hard-coded start date, 2026-10-01). Stores `eur_to_currency` (1 EUR = X).
- `data_quality/Bronze Data Quality Check.ipynb` is a Databricks validation notebook for the bronze layer (data quality downstream of bronze is covered by dbt tests); `dashboards/*.lvdash.json` is the AI/BI dashboard export, which reads the dbt production marts (`workspace.prod_shop_marts.agg_daily_revenue`, `agg_category_revenue`).
- One-time setup: `setup/01_setup_schema.sql` creates schema `workspace.shop` and volume `raw_files`. Run via Databricks.
- Daily schedule (Europe/Prague): Lakeflow Job `shop_daily` (defined in the bundle, see below) loads bronze at 17:00 → dbt Cloud production job at 17:30 → AI/BI dashboard refresh at 18:00. The steps are separately scheduled, not chained, so a change that makes one step run longer can push it past the next step's start time.

## Databricks Asset Bundle (`databricks.yml`, `resources/`)

The Lakeflow pipeline (`resources/shop_pipeline.pipeline.yml`) and the `shop_daily` job (`resources/shop_daily.job.yml`) are defined as a bundle named `shop_pipeline`. dbt is not part of the bundle; it is deployed by dbt Cloud.

- `dev` (default, `mode: development`): resources are named `[dev <user>] ...`, the job schedule is paused, the pipeline runs in development mode and writes to its own schema `workspace.dev_<user>_shop` (created by the bundle). It still reads raw files from the prod volume `/Volumes/workspace/shop/raw_files/`.
- `prod` (`mode: production`): catalog `workspace`, schema `shop`, job scheduled at 17:00 Europe/Prague.

Commands:
- `databricks bundle validate -t dev` / `databricks bundle validate -t prod` — run after every change to `databricks.yml` or `resources/`.
- `databricks bundle deploy -t dev`, `databricks bundle run -t dev shop_daily` — deploy and test your dev copy.
- `databricks bundle destroy -t dev` — remove your dev copy.

**Never deploy to prod from a laptop** (`databricks bundle deploy -t prod`, `bind -t prod`, `destroy -t prod`). Prod deploys will come from CI (GitHub Actions). Validating the prod target locally is fine.

Documented exception: one-time manual bind of the existing prod pipeline and job was done on 2026-11-07; all later prod deploys come from CI only. (`databricks bundle deployment bind shop_pipeline f214ee88-3fd3-42a4-be3b-0f003bd224cf -t prod` and `databricks bundle deployment bind shop_daily 425019135502946 -t prod`.)

## dbt project (`dbt/`)

All dbt files live in `dbt/`: `dbt_project.yml`, `packages.yml`, `package-lock.yml`, `models/`, `macros/`, `tests/`, `analyses/`, `seeds/`, `snapshots/`. Paths in this section are relative to `dbt/`. The dbt Cloud project must have **Project subdirectory** set to `dbt`.

dbt runs in **dbt Cloud**: dbt Studio for development, a deployment job for production. There is no local `profiles.yml`, so **do not run dbt commands locally** — only edit the files; the user runs and tests them in dbt Cloud. Target is Databricks, so the SQL dialect is Spark/Databricks SQL (`qualify`, `explode(sequence(...))`, `last_value(x, true)`). `packages.yml` pulls in `dbt_utils`, which the tests depend on.

Layering (configured in `dbt_project.yml`):
- `staging/shop/` — views, schema `staging`. `stg_shop__orders` does not drop invalid rows; it adds an `is_valid` flag. Filtering happens in `fct_orders` (`where orders.is_valid`).
- `intermediate/` — views, schema `intermediate`. `int_rates_filled` builds a date × currency calendar from the first rate date to `current_date()` and forward-fills the last published ECB rate (`is_filled` marks filled days), plus EUR=1.0, so weekend/holiday orders get a rate instead of failing.
- `marts/finance/` — tables, schema `marts`. `fct_orders` → `agg_daily_revenue`, `agg_category_revenue` (paid only).

Custom schemas use dbt's default `generate_schema_name` (no override in `macros/`), so they resolve to `<target_schema>_staging` etc. Dev target schema is `dbt_<username>_shop` (e.g. `dbt_agrishina_shop_marts`); production is `prod_shop` (`prod_shop_staging`, `prod_shop_intermediate`, `prod_shop_marts`). The production job runs `dbt source freshness`, then `dbt build` (`dbt deps` runs automatically).

Conventions:
- `source()` only in staging models; `ref()` everywhere else.
- Every model starts with import CTEs (`select * from {{ ref(...) }}` / `{{ source(...) }}`) and ends with a `final` CTE followed by `select * from final`.
- Primary keys: single-column keys get `unique` + `not_null`; composite keys get `dbt_utils.unique_combination_of_columns` + `not_null` on every key column (the combination test does not check for nulls).
- Tests go under `data_tests:` and their parameters always go inside `arguments:` (Fusion syntax).
- YAML files are named `_src_*.yml`, `_stg_*.yml`, `_int_models.yml`, `_<folder>__models.yml`.
- Code comments (SQL, Python, YAML) are written in English.

## Workflow

`main` is protected: work on `feature/*` / `chore/*` / `docs/*` branches and merge via pull request (no force pushes). `sample_data/` contains synthetic CSVs with intentional data-quality issues (nulls, zero quantity, negative price, duplicates) — these are expected, not bugs.

## CI (`.github/workflows/ci.yml`)

Runs on every pull request to `main`. It validates and tests only — it never deploys.
- Job `bundle`: installs the Databricks CLI (`databricks/setup-cli`) and runs `databricks bundle validate -t dev` and `-t prod`.
- Job `dbt`: installs `dbt-databricks` (pip, pinned), writes a `profiles.yml` that reads credentials via `env_var()`, and runs `dbt deps` + `dbt build` in `dbt/` against catalog `workspace`, schema `dbt_ci_pr_<PR number>` (custom schemas become `dbt_ci_pr_<N>_staging`, `_intermediate`, `_marts`). A final `if: always()` step drops every `dbt_ci_pr_<N>*` schema, even when the build fails or the run is cancelled. Sources are the prod bronze tables in `workspace.shop` (read-only).
- Secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN` (personal access token; Free Edition has no service principals), `DATABRICKS_HTTP_PATH` (SQL warehouse). Never `echo` them or write them into files.
- CI uses dbt Core + `dbt-databricks`, while dbt Cloud may use Fusion. Keep YAML compatible with both (e.g. test parameters under `arguments:`, which dbt Core ≥ 1.10.5 also accepts).
