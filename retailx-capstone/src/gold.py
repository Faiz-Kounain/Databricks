# Databricks notebook source
# ═══════════════════════════════════════════════════════
# GOLD: Star Schema for analytics
# ═══════════════════════════════════════════════════════

# ── Gold Dim: Current customer versions
spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.gold_dim_customer AS
    SELECT
        customer_id,
        name,
        email,
        city,
        loyalty_tier
    FROM workspace.default.silver_dim_customer
    WHERE __is_current = true
""")
print("gold_dim_customer created")

# ── Gold Fact: Sales with time-based customer join
spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.gold_fact_sales AS
    SELECT
        o.order_id,
        o.order_date,
        o.customer_id,
        c.name AS customer_name,
        c.city AS customer_city,
        c.loyalty_tier AS customer_tier,
        o.product,
        o.quantity,
        o.unit_price,
        o.line_amount
    FROM workspace.default.silver_fact_orders o
    JOIN workspace.default.silver_dim_customer c
        ON o.customer_id = c.customer_id
        AND o.order_date BETWEEN c.__start_date AND c.__end_date
""")
print(f"gold_fact_sales: {spark.sql('SELECT COUNT(*) AS c FROM workspace.default.gold_fact_sales').first()['c']} rows")

# ── Gold Aggregation: Daily revenue by city
spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.gold_daily_revenue AS
    SELECT
        order_date,
        customer_city,
        COUNT(DISTINCT order_id) AS num_orders,
        SUM(quantity) AS total_units,
        SUM(line_amount) AS total_revenue,
        AVG(line_amount) AS avg_order_value
    FROM workspace.default.gold_fact_sales
    GROUP BY order_date, customer_city
""")
print("gold_daily_revenue created")

# ── Gold: Customer 360 (lifetime value)
spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.gold_customer_360 AS
    SELECT
        c.customer_id,
        c.name,
        c.city AS current_city,
        c.loyalty_tier,
        COUNT(o.order_id) AS total_orders,
        COALESCE(SUM(o.line_amount), 0) AS lifetime_value,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date
    FROM workspace.default.gold_dim_customer c
    LEFT JOIN workspace.default.gold_fact_sales o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.name, c.city, c.loyalty_tier
""")
print("gold_customer_360 created")

#spark.sql('SELECT * FROM workspace.default.gold_customer_360 ORDER BY lifetime_value DESC').show()