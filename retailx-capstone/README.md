# RetailX Capstone Project — End-to-End Data Engineering on Databricks

## Project Overview

A production-grade retail analytics pipeline built on Databricks using the Medallion Architecture. Demonstrates SCD Type 2, Star Schema, data quality enforcement, and CI/CD with Asset Bundles.

---

## Business Problem

A retail company (RetailX) needs:

* **Two-source integration** — Orders and customers come from different systems (e-commerce and CRM).
* **Historical tracking** — Customers move and change loyalty tiers over time.
* **Time-aware reporting** — Daily revenue must reflect where the customer was **at the time of the order**, not their current location.
* **Audit trail** — Full customer change history for regulatory compliance.
* **Automated deployment** — Code changes reviewed via PR and deployed via CI/CD.

---

## Architecture

```text
┌──────────────────────────────────────────────────────────┐
│ BRONZE (raw)                                              │
│ bronze_customers ← CDC events from CRM                   │
│ bronze_orders ← Order events from e-commerce             │
└─────────────────────┬────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────┐
│ SILVER (integrated)                                       │
│ silver_dim_customer ← SCD Type 2 dimension               │
│ silver_fact_orders ← Cleaned fact table                  │
└─────────────────────┬────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────┐
│ GOLD (business-ready — Star Schema)                      │
│ gold_dim_customer ← Current customer versions           │
│ gold_fact_sales ← Fact + time-based dim join             │
│ gold_daily_revenue ← Aggregated for BI                   │
│ gold_customer_360 ← Customer lifetime view               │
└──────────────────────────────────────────────────────────┘
```

---

## Concepts Demonstrated

* Full Medallion Architecture (Bronze → Silver → Gold)
* SCD Type 2 dimension with historical tracking
* Star Schema in Gold layer
* Time-based joins for historical accuracy
* Data quality checks with assertions
* Incremental processing (only new events)
* Git version control via Databricks Repos
* CI/CD-ready structure with Asset Bundles
* Automated deployment via GitHub Actions

---

## Pipeline Stages

### Bronze Layer (Raw Ingestion)

* `bronze_customers` — Raw customer CDC events from CRM
* `bronze_orders` — Raw order events from e-commerce system
* Includes ingestion metadata (`_ingested_at`)
* No transformations — preserves source truth

### Silver Layer (Integrated & Cleansed)

* `silver_dim_customer` — SCD Type 2 dimension with columns:

  * `customer_id`, `name`, `email`, `city`, `loyalty_tier`
  * `__start_date`, `__end_date`, `__is_current`, `__updated_at`
* `silver_fact_orders` — Cleaned fact with:

  * Row-level filters (status = 'completed', quantity > 0, unit_price > 0)
  * Derived column (`line_amount = quantity × unit_price`)

### Gold Layer (Business-Ready)

* `gold_dim_customer` — Only current customer versions
* `gold_fact_sales` — Sales fact joined with time-based customer lookup
* `gold_daily_revenue` — Aggregated revenue by (date, city)
* `gold_customer_360` — Customer lifetime value view

---

## Interview Story

> "I built an end-to-end retail analytics pipeline on Databricks using the Medallion Architecture. Bronze ingested raw CDC events from CRM and order systems. Silver built an SCD Type 2 customer dimension — full history — plus a cleaned order fact table. Gold contained a Star Schema: current customer dimension, a sales fact table joined using time-based SCD logic, and two aggregations — daily revenue by city and customer 360. Data quality was enforced with checks in Silver. The pipeline ran daily via a Databricks Job with email alerts on failure. I also set up CI/CD using Databricks Asset Bundles and GitHub Actions — so pushing to main auto-deployed the job."

---

## CI/CD Flow

```text
┌────────────────────────────────────────────┐
│ Developer edits file in Databricks         │
└────────────────┬───────────────────────────┘
                 │
                 │ Commit + Push
                 ▼
┌────────────────────────────────────────────┐
│ GitHub receives push to main               │
└────────────────┬───────────────────────────┘
                 │
                 │ Triggers workflow
                 ▼
┌────────────────────────────────────────────┐
│ GitHub Actions workflow runs               │
│ - Installs Databricks CLI                  │
│ - Runs `databricks bundle deploy`           │
└────────────────┬───────────────────────────┘
                 │
                 │ Deploys to Databricks
                 ▼
┌────────────────────────────────────────────┐
│ Job updated in Databricks workspace        │
│ (no manual click needed)                   │
└────────────────────────────────────────────┘
```

---

## Project Structure

```text
retailx-capstone/
├── src/
│   ├── bronze.py                  ← Bronze layer logic
│   ├── silver.py                  ← Silver layer (SCD Type 2)
│   └── gold.py                    ← Gold layer (Star Schema)
├── tests/
│   └── test_transformations.py
├── .github/
│   └── workflows/
│       ├── ci.yml                 ← CI: tests on pull request
│       └── deploy.yml             ← CD: deploy on push to main
├── databricks.yml                 ← Asset Bundle config
└── README.md
```

---

## Key Takeaways

| Layer  | Purpose                          | Tools Used                    |
| ------ | -------------------------------- | ----------------------------- |
| Bronze | Raw ingestion with metadata      | Delta tables, source reads    |
| Silver | Integration + SCD Type 2 + clean | MERGE statements, ROW_NUMBER  |
| Gold   | Business-ready analytics         | Star Schema, time-based joins |
| CI/CD  | Automated deploy                 | Asset Bundles, GitHub Actions |

---

## SCD Type 2 — The Core Concept

**The 3 magic columns:**

* `__start_date` — When this version became active
* `__end_date` — When this version ended (or `9999-12-31` if current)
* `__is_current` — Boolean flag for the active version

**The 2-step MERGE pattern:**

1. **Close old versions** — set `__is_current = false` and `__end_date`
2. **Insert new versions** — add a new row with `__is_current = true`

**The time-based join:**

```sql
JOIN dim_customer c
  ON o.customer_id = c.customer_id
  AND o.order_date BETWEEN c.__start_date AND c.__end_date
```
