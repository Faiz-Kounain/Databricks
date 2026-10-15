# Databricks notebook source
# ═══════════════════════════════════════════════════════
# SILVER: SCD Type 2 dimension + cleaned fact
# ═══════════════════════════════════════════════════════

# ── Step 1: Create the SCD Type 2 dimension table (if not exists)
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.default.silver_dim_customer (
        customer_id INT,
        name STRING,
        email STRING,
        city STRING,
        loyalty_tier STRING,
        __start_date DATE,
        __end_date   DATE,
        __is_current BOOLEAN,
        __updated_at TIMESTAMP
    )
""")

# ── Step 2: Seed the dimension with initial versions (first time only)
existing_count = spark.sql("SELECT COUNT(*) AS c FROM workspace.default.silver_dim_customer").first()["c"]

if existing_count == 0:
    spark.sql("""
        INSERT INTO workspace.default.silver_dim_customer
        SELECT
            customer_id,
            name,
            email,
            city,
            loyalty_tier,
            CAST(updated_at AS DATE) AS __start_date,
            DATE('9999-12-31')       AS __end_date,
            true                     AS __is_current,
            updated_at               AS __updated_at
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at ASC) AS rn
            FROM workspace.default.bronze_customers
        ) t
        WHERE rn = 1
    """)
    print(" Seeded initial customer versions")
else:
    print(f" Dimension already has {existing_count} rows — skipping seed")

# ── Step 3: Get new events (excluding initial seeds) to process
spark.sql("""
    CREATE OR REPLACE TEMP VIEW new_customer_events AS
    SELECT
        customer_id, name, email, city, loyalty_tier, operation, updated_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at ASC) AS rn
        FROM workspace.default.bronze_customers
    ) t
    WHERE rn > 1
    -- Only events newer than what's already in the dimension
    AND updated_at > COALESCE(
        (SELECT MAX(__updated_at) FROM workspace.default.silver_dim_customer),
        TIMESTAMP('1900-01-01')
    )
""")

new_count = spark.sql("SELECT COUNT(*) AS c FROM new_customer_events").first()["c"]
print(f" {new_count} new customer events to process")

# ── Step 4: SCD Type 2 — Close old versions
if new_count > 0:
    spark.sql("""
        MERGE INTO workspace.default.silver_dim_customer AS t
        USING (
            SELECT customer_id, name, email, city, loyalty_tier, updated_at
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
                FROM new_customer_events
            ) x
            WHERE rn = 1
        ) AS s
        ON t.customer_id = s.customer_id AND t.__is_current = true
        WHEN MATCHED AND (
            t.name != s.name OR
            t.email != s.email OR
            t.city != s.city OR
            t.loyalty_tier != s.loyalty_tier
        ) THEN
            UPDATE SET
                t.__is_current = false,
                t.__end_date = CAST(s.updated_at AS DATE)
    """)

    # ── Step 5: SCD Type 2 — Insert new versions
    spark.sql("""
        INSERT INTO workspace.default.silver_dim_customer
        SELECT
            s.customer_id,
            s.name,
            s.email,
            s.city,
            s.loyalty_tier,
            CAST(s.updated_at AS DATE) AS __start_date,
            DATE('9999-12-31')         AS __end_date,
            true                       AS __is_current,
            s.updated_at               AS __updated_at
        FROM (
            SELECT customer_id, name, email, city, loyalty_tier, updated_at
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
                FROM new_customer_events
            ) x
            WHERE rn = 1
        ) s
        LEFT JOIN workspace.default.silver_dim_customer t
            ON t.customer_id = s.customer_id AND t.__is_current = true
        WHERE t.customer_id IS NULL
    """)

    print("Processed new customer events")

# ── Step 6: Build silver_fact_orders (cleaned, with derived columns)
spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.silver_fact_orders AS
    SELECT
        order_id,
        customer_id,
        order_date,
        product,
        quantity,
        unit_price,
        quantity * unit_price AS line_amount,
        status
    FROM workspace.default.bronze_orders
    WHERE status = 'completed'
      AND quantity > 0
      AND unit_price > 0
""")

print(f"silver_fact_orders: {spark.sql('SELECT COUNT(*) AS c FROM workspace.default.silver_fact_orders').first()['c']} rows")

#Add Expectations

# Rebuild silver_fact_orders with quality checks
spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.silver_fact_orders AS
    SELECT
        order_id,
        customer_id,
        order_date,
        product,
        quantity,
        unit_price,
        quantity * unit_price AS line_amount,
        status
    FROM workspace.default.bronze_orders
    WHERE status = 'completed'
      AND quantity > 0
      AND unit_price > 0
""")

# Verify no bad rows
bad_rows = spark.sql("""
    SELECT COUNT(*) AS c FROM workspace.default.silver_fact_orders
    WHERE quantity <= 0 OR unit_price <= 0
""").first()["c"]

assert bad_rows == 0, f" Data quality check failed: {bad_rows} bad rows"
print(" Data quality check passed")

#Add a Referential Integrity Check

# Check: every order's customer exists in dim
orphan_orders = spark.sql("""
    SELECT COUNT(*) AS c
    FROM workspace.default.silver_fact_orders o
    LEFT JOIN workspace.default.silver_dim_customer c
        ON o.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""").first()["c"]

assert orphan_orders == 0, f" {orphan_orders} orders have no matching customer"
print(" Referential integrity check passed")
