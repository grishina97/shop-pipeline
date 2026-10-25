# shop-pipeline

End-to-end data pipeline on **Databricks** (Lakeflow Spark Declarative Pipelines) for an online shop:
daily order files and an exchange-rate API are ingested, cleaned and combined into revenue tables in EUR.
Built with a medallion architecture (bronze → silver → gold) and managed with Git / GitHub pull requests.

## Architecture

```mermaid
flowchart LR
    A[CSV files<br/>Unity Catalog Volume] -->|Auto Loader| B[bronze_orders]
    C[Frankfurter API<br/>ECB exchange rates] -->|requests| D[bronze_rates]
    B --> E[silver_orders]
    D --> F[silver_rates]
    E --> G[gold_orders_eur]
    F --> G
    G --> H[gold_daily_revenue]
    G --> I[gold_category_revenue]
```

## Data sources

| Source | Description | Ingestion |
|---|---|---|
| Order files | **Synthetic** daily CSV files (`orders_YYYY-MM-DD.csv`) in 5 currencies: CZK, EUR, USD, GBP, PLN, generated with intentional data quality issues (nulls, zero quantity, negative price, duplicates) | Auto Loader (incremental, only new files) |
| [Frankfurter API](https://frankfurter.dev) | Daily reference exchange rates published by the European Central Bank | HTTP request, full refresh |

## Tables

| Layer | Table | Type | Description |
|---|---|---|---|
| Bronze | `bronze_orders` | Streaming table | Raw orders as received + `source_file`, `ingested_at` |
| Bronze | `bronze_rates` | Materialized view | Raw rates from the API (1 EUR = X currency) |
| Silver | `silver_orders` | Streaming table | Typed, deduplicated orders; invalid rows dropped |
| Silver | `silver_rates` | Materialized view | Rates converted to `rate_to_eur` + EUR = 1.0 |
| Gold | `gold_orders_eur` | Materialized view | Each order with amount converted to EUR |
| Gold | `gold_daily_revenue` | Materialized view | Orders and revenue (EUR) by date and country |
| Gold | `gold_category_revenue` | Materialized view | Orders and revenue (EUR) by product category |

## Dashboard

AI/BI dashboard on gold tables: KPIs, revenue by day, country and category, with date and country filters.

![Dashboard](docs/dashboard.png)

## Orchestration

Lakeflow Job `shop_daily`: runs the pipeline, then refreshes the dashboard.
Scheduled daily at 17:00 Europe/Prague (after ECB rates are published ~16:00 CET), email notification on failure.

## Data quality

- **silver_orders** – `expect_or_drop`: customer_id not null, quantity > 0, unit_price > 0; duplicates removed by `order_id`
- **silver_rates** – `expect_or_fail`: rate_to_eur > 0 (a wrong rate would corrupt all revenue)
- **gold_orders_eur** – `FAIL UPDATE` if an order has no matching exchange rate (revenue is never lost silently)
- Data quality check notebooks for each layer: row counts, duplicates, nulls, invalid values, bronze → silver reconciliation

## Example run

Third daily file processed incrementally:
- `bronze_orders`: 151 new rows (only the new file was read)
- `silver_orders`: 146 rows (4 dropped by expectations, 1 duplicate removed)
- `gold_orders_eur`: 438 orders across 3 days, all converted to EUR 

## Repository structure

```text
docs/
  dashboard.png                  # dashboard screenshot
pipeline/shop-pipeline/
  transformations/               # pipeline source code
    bronze_orders.py             # Auto Loader: CSV files → bronze
    bronze_rates.py              # Frankfurter API → bronze
    silver_orders.py             # cleaning, types, expectations, dedup
    silver_rates.py              # rates to EUR + EUR row
    gold.sql                     # orders in EUR, daily and category revenue
  data_quality/                  # validation notebooks for each layer
    Bronze Data Quality Check
    Silver Data Quality Check
    Gold Data Quality Check
  dashboards/
    Shop Revenue                 # AI/BI dashboard on gold tables
sample_data/                     # synthetic daily order files
  orders_2026-10-20.csv
  orders_2026-10-21.csv
  orders_2026-10-22.csv
setup/
  01_setup_schema.sql            # schema and volume (run once)
README.md
```

## How to run

1. Run `setup/01_setup_schema.sql` to create the `workspace.shop` schema and the `raw_files` volume
2. Upload order CSV files from `sample_data/` to `/Volumes/workspace/shop/raw_files/` (one file per run to see incremental loading)
3. Create an ETL pipeline with root folder `pipeline/shop-pipeline` and default location `workspace.shop`
4. Run the pipeline; upload the next CSV and run again – only the new file is processed

## Development workflow

- Feature branches (`feature/silver`, `feature/api-rates`, `feature/gold`, `feature/dashboard-job`) → pull request → merge into `main`
- `main` is protected: pull request required, force pushes and deletion blocked

## Tech stack

Databricks (Free Edition) · Lakeflow Spark Declarative Pipelines · Lakeflow Jobs · Auto Loader · Delta Lake · Unity Catalog · AI/BI Dashboards · PySpark · SQL · Git / GitHub

## Next steps

- dbt models for the gold layer
- CI/CD with Databricks Asset Bundles and GitHub Actions
