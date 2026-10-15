# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

# ─────────────────────────────────────────────────────
# BRONZE: Raw ingestion with metadata
# ─────────────────────────────────────────────────────

# Bronze customers
df_customers = spark.read.table("workspace.default.src_crm_customers")
df_customers.write \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_customers")

print(f"bronze_customers: {df_customers.count()} rows")

# Bronze orders
df_orders = spark.read.table("workspace.default.src_ecom_orders")
df_orders.write \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_orders")

print(f"bronze_orders: {df_orders.count()} rows")
