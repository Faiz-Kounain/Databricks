# dbt on Databricks 

---

## PART 1: What is dbt?

**dbt (data build tool)** is a transformation framework. You write SQL `SELECT` statements, and dbt compiles them into tables and views in your warehouse.

### The Key Idea

You don't write `CREATE TABLE AS SELECT ...`. You write just the `SELECT`, and dbt handles the rest.

### Where dbt Fits in the ELT Pipeline

Extract → Load → Transform

- Extract: Auto Loader / Connectors
- Load: Delta tables (Bronze)
- Transform: dbt (Silver + Gold)

| Step | Tool | Purpose |
|---|---|---|
| **Extract** | Auto Loader / Connectors | Bring data in |
| **Load** | Delta tables (Bronze) | Store raw data |
| **Transform** | dbt (Silver + Gold) | Clean, join, aggregate |

**dbt assumes data is already loaded.** It never extracts or loads.

### Why dbt Matters

| Without dbt | With dbt |
|---|---|
| Manually write `CREATE TABLE AS SELECT` | Just write `SELECT` |
| Manually order transformations | dbt resolves dependencies via `ref()` |
| Manually test data quality | Built-in testing framework |
| Manually document | Auto-generated docs |
| No version control | Git-integrated |

---

## PART 2: Core Concepts

### 1. Models

A **model** is a `.sql` file containing a **single SELECT statement**.

**Example:** `models/silver/stg_diamonds.sql`

```sql
{{ config(materialized='table') }}

SELECT
    CAST(carat AS DOUBLE) AS carat,
    UPPER(TRIM(cut)) AS cut,
    CAST(price AS INT) AS price
FROM {{ ref('diamonds') }}
WHERE price > 0
```

dbt turns this into:

```sql
CREATE TABLE default_silver.stg_diamonds AS
SELECT ... FROM default.diamonds WHERE price > 0
```

### 2. Materializations

You control how dbt builds each model via the config block.

| Materialization | What It Creates | When to Use |
|---|---|---|
| `view` (default) | A view | Small, fast-changing data |
| `table` | A full table | Frequently queried |
| `incremental` | Only new/changed rows | Large tables (millions+ rows) |
| `ephemeral` | A CTE (no object created) | Intermediate logic only |
| `materialized_view` | Databricks materialized view | Pre-computed aggregates |
| `streaming_table` | Databricks streaming table | Real-time data |

Example:

```sql
{{ config(materialized='table', file_format='delta') }}
```

### 3. `ref()` — The Magic of dbt

Instead of hardcoding table names, you use `{{ ref('model_name') }}`.

```sql
SELECT *
FROM {{ ref('stg_diamonds') }}
```

What `ref()` does:

- Creates a dependency — dbt runs `stg_diamonds` first.
- Resolves the table name at compile time.
- Builds the DAG of transformations automatically.

Analogy: `ref()` is like `import` in Python — it says, "I need this first."

### 4. Tests

Two types of tests in dbt:

#### Schema Tests (declared in YAML)

```yaml
version: 2

models:
  - name: stg_diamonds
    columns:
      - name: carat
        tests:
          - not_null
      - name: cut
        tests:
          - accepted_values:
              arguments:
                values: ['IDEAL', 'PREMIUM', 'GOOD', 'FAIR', 'VERY GOOD']
```

Built-in generic tests:

| Test | Purpose |
|---|---|
| `not_null` | Column must not be null |
| `unique` | Values must be unique |
| `accepted_values` | Values must be in a list |
| `relationships` | Foreign key must exist in another table |

#### Data Tests (custom SQL in `tests/` folder)

```sql
-- tests/assert_price_positive.sql
SELECT *
FROM {{ ref('diamonds') }}
WHERE price <= 0
```

Rule: If the query returns rows, the test FAILS. If it returns nothing, the test PASSES.

---

## PART 3: dbt Project Architecture

### Directory Structure

```text
retailx_dbt/
├── dbt_project.yml        # Project config (name, paths, materializations)
├── models/                # Your SQL transformations
│   ├── silver/
│   │   └── stg_diamonds.sql
│   └── gold/
│       └── fct_diamonds_prices.sql
├── seeds/                 # Static CSV files loaded by dbt
│   └── diamonds.csv
├── tests/                 # Custom SQL tests
│   └── assert_price_positive.sql
├── macros/                # Reusable Jinja functions
├── snapshots/             # SCD Type 2 automation
├── analyses/              # Ad-hoc SQL (compiled but not run)
├── target/                # Compiled output (gitignored)
└── logs/                  # Run logs (gitignored)
```

### File Purposes

| File/Folder | Purpose | Do You Commit It? |
|---|---|---|
| `dbt_project.yml` | Project config — name, paths, default materializations | Yes |
| `profiles.yml` | Connection config — host, token, schema | No (contains secrets) |
| `models/*.sql` | Transformation logic | Yes |
| `seeds/*.csv` | Static data files | Yes |
| `tests/*.sql` | Custom data tests | Yes |
| `macros/*.sql` | Reusable Jinja macros | Yes |
| `snapshots/*.sql` | SCD Type 2 definitions | Yes |
| `target/` | Compiled SQL | Gitignore |
| `logs/` | Run logs | Gitignore |
| `dbt_packages/` | Installed packages | Gitignore |

### `dbt_project.yml` Structure

```yaml
name: 'retailx_dbt'
version: '1.0.0'
config-version: 2

profile: 'retailx_dbt'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  retailx_dbt:
    silver:
      +materialized: table
      +schema: silver
    gold:
      +materialized: table
      +schema: gold
```

Key blocks:

- `name` — Project name (must match profile reference).
- `profile` — Points to `profiles.yml`.
- Paths — Where dbt looks for each asset type.
- `models:` — Default materializations per folder.

### `profiles.yml` Structure

```yaml
retailx_dbt:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: workspace
      schema: default
      host: dbc-1dda8b01-b539.cloud.databricks.com
      http_path: /sql/1.0/warehouses/d2beae9cbe415476
      token: dapi...
      threads: 4
```

Never commit this file. It contains your token.

---

## PART 4: The dbt Flow

```text
┌────────────────────────────────────────────────────────────┐
│ 1. Developer writes .sql model                             │
│    (with {{ ref() }} and {{ config() }})                   │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│ 2. dbt reads dbt_project.yml + profiles.yml                │
│    Identifies all models, tests, seeds                     │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│ 3. dbt builds the DAG                                      │
│    Parses all {{ ref() }} calls                            │
│    Determines execution order                              │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│ 4. dbt compiles each model to raw SQL                      │
│    Resolves ref() → real table name                        │
│    Applies config() → materialization                      │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│ 5. dbt executes on the target warehouse                    │
│    Runs models in DAG order                                │
│    Runs tests, captures logs                               │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│ 6. Tables/views created in Unity Catalog                   │
│    Example: workspace.default_silver.stg_diamonds          │
│             workspace.default_gold.fct_diamonds_prices     │
└────────────────────────────────────────────────────────────┘
```

---

## PART 5: Practical Setup — Steps We Followed

### Phase 1: Local Setup (Development)

**Purpose:** Learn dbt, iterate fast, debug locally.

Steps:

**1. Install dbt and the adapter:**

```powershell
pip install dbt-core dbt-databricks
```

**2. Create `profiles.yml` in `~/.dbt/`:**

- Host: Workspace URL (no `https://`).
- HTTP path: From SQL Warehouse Connection Details.
- Token: Databricks PAT (starts with `dapi`).
- Catalog: `workspace`.
- Schema: `default`.

**3. Verify connection:**

```powershell
dbt debug
```

Expected: `Connection test: [OK connection ok]`

**4. Create project structure:**

```powershell
mkdir dbt_test\retailx_dbt
cd dbt_test\retailx_dbt
```

**5. Create `dbt_project.yml`** with project name, paths, and materializations.

**6. Create seed:** `seeds/diamonds.csv`

**7. Create Silver model:** `models/silver/stg_diamonds.sql`

**8. Create Gold model:** `models/gold/fct_diamonds_prices.sql`

**9. Create tests:** `models/silver/schema.yml` and `tests/assert_price_positive.sql`

**10. Run the pipeline:**

```powershell
dbt seed    # Load CSV   (CSV seed → diamonds table	Raw data lands in Bronze via Auto Loader)
(Source() → references a raw table that dbt does NOT manage 
SELECT * FROM {{ source('bronze', 'crm_customers') }})
dbt run     # Build models
dbt test    # Validate data
```

### Phase 2: Databricks Bundle (Production)

**Purpose:** Deploy dbt as a scheduled Databricks job, CI/CD-ready.

Steps:

**1. Upload dbt project to Databricks workspace:**

Path: `/Workspace/Users/<you>/Databricks/dbt_projects/retailx_dbt/`

Files: `dbt_project.yml`, `models/`, `seeds/`, `tests/`

**2. Create `databricks.yml` bundle config:**

```yaml
bundle:
  name: retailx-dbt

resources:
  jobs:
    retailx_dbt_pipeline:
      name: "RetailX dbt Pipeline"
      tasks:
        - task_key: dbt_transform
          dbt_task:
            project_directory: ./
            commands:
              - "dbt deps"
              - "dbt seed"
              - "dbt run"
              - "dbt test"
            warehouse_id: <your-warehouse-id>
            catalog: workspace
            schema: default
          environment_key: default
      environments:
        - environment_key: default
          spec:
            client: "2"
            dependencies:
              - "dbt-databricks>=1.6.0"

targets:
  dev:
    default: true
    workspace:
      host: dbc-1dda8b01-b539.cloud.databricks.com
```

**3. Deploy from Databricks UI:** Deployments panel → Deploy.

**4. Verify:** Jobs & Pipelines → RetailX dbt Pipeline → Run.

### Phase 3: CI/CD (GitHub Actions)

**Purpose:** Auto-deploy on push, run tests on PR.

#### Files

**`.github/workflows/cd.yml` — Deploy and Run on push to main**

```yaml
name: CD - Deploy and Run dbt

on:
  push:
    branches: [main]
    paths:
      - 'dbt_projects/retailx_dbt/**'
  workflow_dispatch:

jobs:
  deploy-and-run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

      - name: Deploy
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          cd dbt_projects/retailx_dbt
          databricks bundle deploy --target dev

      - name: Run
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          cd dbt_projects/retailx_dbt
          databricks bundle run retailx_dbt_pipeline --target dev
```

**`.github/workflows/ci.yml` — Validate on PR**

```yaml
name: CI - Validate dbt

on:
  pull_request:
    branches: [main]
    paths:
      - 'dbt_projects/retailx_dbt/**'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

      - name: Validate
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          cd dbt_projects/retailx_dbt
          databricks bundle validate --target dev
```

**GitHub Secrets (Repository → Settings → Secrets and variables → Actions):**

| Name | Value |
|---|---|
| `DATABRICKS_HOST` | `https://dbc-xxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Databricks PAT (`dapi...`) |

---

## PART 6: Errors We Hit and Fixes

| Error | Cause | Fix |
|---|---|---|
| Access to the path `.dbt` is denied | PowerShell `%USERPROFILE%` syntax in system folder | Use `$env:USERPROFILE` and `cd` first |
| `401 Unauthorized` | Wrong/expired token | Regenerate Databricks PAT |
| `host: https://.../` in `profiles.yml` | Trailing slash / HTTPS prefix | Remove both — use bare hostname |
| `ghp_...` token in dbt profile | GitHub token instead of Databricks | Use `dapi...` Databricks PAT |
| Missing `bundle.name` | Wrong indentation — `workspace:` nested under `bundle:` | Move `workspace:` to top level |
| Expected a notebook but got a file | `.py` files are not notebooks | Convert to Databricks notebooks (no `.py`) |
| `MissingArgumentsPropertyInGenericTestDeprecation` | Old test syntax | Wrap values under `arguments:` key |
| Invalid platform channel Client-1 | Free Edition doesn't support Client 1 | Change `client: "1"` to `client: "2"` |
| Cannot launch the cluster | Free Edition limits | Use `client: "2"` |
| Exit status 127 (`dbt: command not found`) | dbt not installed in serverless environment | Add `dbt-databricks>=1.6.0` to dependencies in environments |
| dbt writes to `hive_metastore` | Missing catalog and schema in `dbt_task` | Add `catalog: workspace` and `schema: default` |
| Workflow runs but job doesn't run | Only bundle deploy, no bundle run | Add a second step with `databricks bundle run` |
| Duplicate workflows on push | Two files triggered by the same event | Delete the redundant one |

---

## PART 7: dbt vs DLT — When to Use Which

| Feature | dbt | DLT (Delta Live Tables) |
|---|---|---|
| Language | SQL only (with Jinja) | SQL and Python |
| Syntax | `SELECT` + config | `CREATE OR REFRESH STREAMING TABLE` |
| Dependencies | Automatic via `ref()` | Automatic via `live.` prefix |
| Tests | Schema + data tests | Expectations (WARN/DROP/FAIL) |
| Streaming | Limited | Native |
| Incremental | `materialized='incremental'` | Streaming Tables |
| Best for | Batch transformations, BI | Ingestion, streaming, real-time |
| Orchestration | External (Airflow, GitHub Actions, Databricks Jobs) | Built-in |

In practice, many teams use both. DLT for ingestion; dbt for Silver/Gold transformations.

---

## PART 8: Interview Talking Points

### Q: What is dbt?

"dbt is a transformation framework. You write SQL SELECT statements, and dbt compiles them into tables and views. It handles dependency resolution via `ref()`, provides testing, generates documentation, and integrates with Git. It's the 'T' in ELT."

### Q: How does dbt differ from DLT?

"DLT is Databricks' native pipeline framework with support for streaming and Python. dbt is SQL-only and focused on transformations. DLT is better for ingestion and streaming; dbt is better for batch transformations with strong testing and documentation."

### Q: How does dbt handle dependencies?

"Via `ref()`. When you write `{{ ref('model_name') }}`, dbt parses it, creates a dependency edge in the DAG, and executes models in the correct order. It also resolves the actual table name at compile time — so you never hardcode paths."

### Q: What are materializations?

"They control how dbt builds each model. `view` is the default — no storage. `table` recreates each run. `incremental` only processes new/changed rows using a unique key. Databricks adds `materialized_view` and `streaming_table` for native integration."

### Q: How do you test data quality in dbt?

"Two ways. Schema tests — declared in YAML — check column-level rules like `not_null`, `unique`, and `accepted_values`. Data tests — SQL files in the `tests/` folder — pass when they return zero rows and fail when they return rows. Both run via `dbt test`. In CI, they run on every pull request."

### Q: How do you deploy dbt in production on Databricks?

"We use Databricks Asset Bundles. The dbt project has a `databricks.yml` that defines the job with a `dbt_task` — commands, warehouse, catalog, schema. The bundle is deployed via CI/CD — GitHub Actions runs `databricks bundle deploy` and `databricks bundle run`. It's version-controlled and idempotent."

### Q: What's the difference between a dbt seed, model, snapshot, and source?

"A seed is a static CSV loaded by dbt. A model is a SELECT-based transformation. A snapshot captures historical versions for SCD Type 2. A source is a reference to raw tables that dbt doesn't manage — used for lineage."

### Q: How would you handle SCD Type 2 in dbt?

"Using dbt snapshots. You define a snapshot on a source table with a `unique_key` and `updated_at`. On each dbt snapshot, dbt compares rows and closes old versions with `dbt_valid_to`, adding new rows as needed. It's the native dbt approach to SCD Type 2."

---

## PART 9: Common dbt Commands

| Command | What It Does |
|---|---|
| `dbt init <project>` | Create a new dbt project |
| `dbt debug` | Test connection and config |
| `dbt deps` | Install packages from `packages.yml` |
| `dbt seed` | Load CSV seeds into the warehouse |
| `dbt run` | Execute all models |
| `dbt run --select model_name` | Run one specific model |
| `dbt run --select +model_name` | Run the model and its upstream dependencies |
| `dbt run --select model_name+` | Run the model and its downstream dependencies |
| `dbt test` | Run all tests |
| `dbt snapshot` | Capture SCD Type 2 snapshots |
| `dbt docs generate` | Generate documentation |
| `dbt docs serve` | Serve docs locally |
| `dbt build` | Run seeds, models, tests, and snapshots in order |
| `dbt clean` | Remove `target/` and `dbt_packages/` |

---

## PART 10: Best Practices

| Practice | Why |
|---|---|
| Use `ref()` everywhere | Enables DAG, resolves names correctly |
| Layer models — staging → intermediate → marts | Clear separation, easier debugging |
| One model = one purpose | Easier testing, clearer lineage |
| Test critical columns | Catch bad data early |
| Commit dbt to Git | Version control, CI/CD |
| Never commit `profiles.yml` | Contains secrets |
| Use `.gitignore` for `target/`, `logs/`, `dbt_packages/` | Avoid committing generated files |
| Pin package versions | Avoid surprise breaking changes |
| Naming conventions: `stg_`, `int_`, `fct_`, `dim_` | Clear prefixes by layer |
| Use incremental models for large tables | Performance and cost savings |
| Document key models | Team collaboration |
| Run `dbt build` in CI | One command runs everything |

---

## PART 11: Key Terminology

| Term | Meaning |
|---|---|
| Model | A `.sql` file with one SELECT statement |
| Seed | A CSV file that dbt loads as a table |
| Snapshot | Historical version tracking (SCD Type 2) |
| Source | Reference to an external/raw table |
| Ref | Reference to another dbt model |
| Materialization | How a model is built (table/view/incremental) |
| Macro | Reusable Jinja function |
| DAG | Directed Acyclic Graph of model dependencies |
| Profile | Connection config (host, token, catalog) |
| Target | Named environment (dev/prod) |
| Package | External dbt code (e.g., `dbt_utils`) |
| Jinja | Templating language used in dbt SQL |

---

## PART 12: Complete Working Project (RetailX Example)

### Files Created

```text
dbt_projects/retailx_dbt/
├── dbt_project.yml
├── databricks.yml
├── seeds/
│   └── diamonds.csv                 (20 rows)
├── models/
│   ├── hello_dbt.sql                (basic demo)
│   ├── silver/
│   │   ├── stg_diamonds.sql         (cleaned)
│   │   └── schema.yml               (tests)
│   └── gold/
│       └── fct_diamonds_prices.sql  (aggregated)
├── tests/
│   └── assert_price_positive.sql    (custom test)
└── .github/
    └── workflows/
        ├── ci.yml                   (validation on PR)
        └── cd.yml                   (deploy and run on push)
```

### Data Flow

```text
diamonds.csv ──seed──▶ diamonds (table)
                           │
                           │ ref()
                           ▼
diamonds ──transform──▶ stg_diamonds (Silver)
                           │
                           │ ref()
                           ▼
stg_diamonds ──aggregate──▶ fct_diamonds_prices (Gold)
```

### The Full Stack

```text
┌────────────────────────────────────────────────────────┐
│ Local Development                                      │
│ - dbt + dbt-databricks via pip                         │
│ - profiles.yml for connection                          │
│ - dbt run / test locally                               │
└────────────────────┬───────────────────────────────────┘
                     │ git push
                     ▼
┌────────────────────────────────────────────────────────┐
│ GitHub                                                 │
│ - Version control                                      │
│ - Actions workflows (ci.yml, cd.yml)                   │
│ - Secrets: DATABRICKS_HOST, DATABRICKS_TOKEN           │
└────────────────────┬───────────────────────────────────┘
                     │ triggers
                     ▼
┌────────────────────────────────────────────────────────┐
│ GitHub Actions Runner                                  │
│ - Checkout code                                        │
│ - Install Databricks CLI                               │
│ - databricks bundle deploy                             │
│ - databricks bundle run                                │
└────────────────────┬───────────────────────────────────┘
                     │ deploys to
                     ▼
┌────────────────────────────────────────────────────────┐
│ Databricks Workspace                                   │
│ - Job: "RetailX dbt Pipeline v2"                       │
│ - Runs dbt deps → seed → run → test                    │
│ - Uses Unity Catalog (workspace.default)               │
└────────────────────────────────────────────────────────┘
```

---

## PART 13: What's Next in dbt (Advanced Topics)

| Topic | What It Is | When You Need It |
|---|---|---|
| Incremental Models | Process only new/changed rows via `unique_key` | Large tables (millions+ rows) |
| Snapshots | Native SCD Type 2 with `dbt_valid_from` / `dbt_valid_to` | Historical dimension tracking |
| Jinja & Macros | `{% for %}`, `{% if %}`, reusable functions | DRY SQL, dynamic logic |
| Sources & Lineage | Define raw tables as sources in YAML | Documentation, impact analysis |
| Packages | `dbt_utils`, `dbt_expectations`, etc. | Common utilities |
| Exposures | Document downstream dashboards/tools | Full lineage to consumption |
| dbt Tests (advanced) | Custom generic tests, parameterized tests | Team-standard testing |
| dbt Cloud | Managed dbt with scheduler, IDE, docs hosting | Enterprise teams |
| `dbt docs generate` | Auto-generate documentation site | Team collaboration |
| Model Contracts | Enforce schema for downstream consumers | Data platform governance |

---

##Incremental model
```
Problem: materialized='table' rebuilds the whole table on every run. With 10 rows, that’s fine. With 2 billion orders, rebuilding everything daily is slow and costly when only 50,000 rows are new.

Incremental = the first run builds the full table, and every later run processes only the new or changed rows and adds them in.

The model
{{ config(
    materialized = 'incremental',
    unique_key   = 'order_id',            -- how to recognise the "same" row
    incremental_strategy = 'merge'        -- default on Databricks
) }}

SELECT order_id, customer_id, amount, updated_at
FROM {{ source('bronze', 'orders') }}

{% if is_incremental() %}
  -- only on later runs: rows newer than what we already have
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
is_incremental() is false on the first run (the table doesn’t exist yet), so the WHERE is skipped and everything loads. On every later run it’s true, so the filter applies.
{{ this }} means “this model’s own table”, the target. So MAX(updated_at) is the watermark: “how far did I get last time?”
unique_key is the key used to update existing rows instead of duplicating them.
```

##scd 2 logic in dbt

```
Snapshots. That’s dbt’s built-in SCD2. You write a SELECT, and dbt handles the “close the old row, insert the new row” MERGE for you.

1. Create snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}

{{ config(
    target_schema = 'snapshots',
    unique_key    = 'customer_id',
    strategy      = 'timestamp',
    updated_at    = 'updated_at'
) }}

SELECT customer_id, name, city, updated_at
FROM {{ source('bronze', 'customers') }}

{% endsnapshot %}
2. Run it
dbt snapshot

Run this every day, before dbt run. Or just use dbt build, which runs snapshots in the right order.

3. What you get

dbt adds 4 columns automatically:

customer_id	city	dbt_valid_from	dbt_valid_to	dbt_scd_id	dbt_updated_at
C1	Pune	2026-10-01	2026-10-02	a1b2…	2026-10-01
C1	Delhi	2026-10-02	null	c3d4…	2026-10-02
dbt_valid_to IS NULL = current row (your is_current = true).
dbt_scd_id = a unique ID per version, which you can use as a surrogate key.
Two strategies
Strategy	How dbt detects a change	Use when
timestamp	updated_at is newer than last time	Source has a reliable updated-at column (preferred)
check	Compares the columns you list	No timestamp in source
{{ config(unique_key='customer_id', strategy='check', check_cols=['name','city']) }}
Useful options
invalidate_hard_deletes=True (newer versions: hard_deletes='invalidate'): if a customer disappears from the source, dbt closes their row.
Snapshots are never rebuilt by --full-refresh, because the history can’t be recreated from the source.
Use it in your Gold model
-- dim_customer.sql
SELECT dbt_scd_id AS customer_key, customer_id, name, city,
       dbt_valid_from AS start_date, dbt_valid_to AS end_date,
       dbt_valid_to IS NULL AS is_current
FROM {{ ref('customers_snapshot') }}

Then the fact joins on order_date >= start_date AND (end_date IS NULL OR order_date < end_date), exactly like your hand-written star schema.
```


## PART 14: The Interview-Ready Elevator Pitch

"dbt is a transformation framework that turns SQL SELECT statements into tables and views. We use it for the transformation layer in our ELT pipeline — the T after Extracting and Loading raw data into Delta.

The key concepts are models, materializations, refs, and tests. Models are `.sql` files. Materializations control how they're built — table, view, or incremental. `ref()` creates dependencies and builds the DAG. Tests validate data quality — schema tests in YAML, custom data tests in SQL.

On Databricks, we package dbt projects as Asset Bundles with a `dbt_task`. It runs on serverless compute with `dbt-databricks` installed as a dependency. The whole thing is version-controlled in Git, and deployed via GitHub Actions on push to main. Tests run on pull requests; deploy and run happen on merge.

I've used it in a Medallion architecture — dbt builds the Silver and Gold layers from Bronze data, with tests enforcing quality at every step."