# Running on Databricks (Free Edition works)

> The Databricks path is written to Databricks' documented APIs. CI parses the dbt
> project for the Databricks target and runs the same SQL on Apache Spark, but it has not
> yet been run against a live workspace. Run through it once and fix anything your
> workspace flags.

## 1. Workspace and CLI

1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
2. Install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) and log in:
   ```bash
   databricks auth login --host https://<your-workspace>.cloud.databricks.com
   ```
3. Find your SQL warehouse ID: **SQL Warehouses → (your warehouse) → Connection details**.
   It is the last part of the HTTP path.

## 2. Deploy the bundle

```bash
databricks bundle validate --var warehouse_id=<id>
databricks bundle deploy   --var warehouse_id=<id>
databricks bundle run uk_property_monthly --var warehouse_id=<id>
```

This creates:
- the schema `workspace.uk_property` and the Volume `workspace.uk_property.landing`
- a job with two tasks:
  - `ingest` on serverless compute: backfills 3 years on the first run, then lands the
    monthly update into the Volume
  - `dbt_build` on your SQL warehouse: builds `bronze`, `silver` and `gold` as Delta
    tables and runs every data test

## 3. Running dbt from your laptop against Databricks (optional)

```bash
pip install -e ".[databricks]"
export DBT_TARGET=databricks
export DATABRICKS_HOST=<workspace>.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>
export DATABRICKS_TOKEN=<personal access token>
export LANDING_URI=/Volumes/workspace/uk_property/landing

lakehouse monthly            # uploads to the Volume through the Files API
dbt build --project-dir transform --profiles-dir transform --exclude-resource-type unit_test
```

Unit tests are excluded here because their fixtures use DuckDB syntax; they run in CI.

## 4. Things to show in a demo

- **Catalog Explorer → workspace.silver.silver_transactions → History.** Each monthly
  `MERGE` is a Delta version. You can time-travel with
  `SELECT * FROM ... VERSION AS OF 3`.
- **Lineage tab** on a gold table: Unity Catalog traces it back through silver and bronze
  to the files in the Volume.
- A **Databricks AI/BI dashboard** on the gold tables, to compare with the static site.
