# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %sql
# MAGIC -- ====================================================================
# MAGIC -- 1. HIERARCHY SETUP
# MAGIC -- ====================================================================
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce;
# MAGIC USE CATALOG ecommerce;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC
# MAGIC -- ====================================================================
# MAGIC -- 2. CREATE MANAGED TABLES (Safe for all environments)
# MAGIC -- ====================================================================
# MAGIC CREATE TABLE IF NOT EXISTS bronze.events (
# MAGIC   event_id INT,
# MAGIC   user_id STRING,
# MAGIC   event_type STRING,
# MAGIC   event_time TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.events (
# MAGIC   event_id INT,
# MAGIC   user_id STRING,
# MAGIC   event_type STRING,
# MAGIC   standard_time TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.products (
# MAGIC   product_name STRING,
# MAGIC   revenue DOUBLE,
# MAGIC   purchases INT,
# MAGIC   conversion_rate DOUBLE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- ====================================================================
# MAGIC -- 3. CORRECTED PERMISSIONS (Metastore 1.0 Compatible)
# MAGIC -- ====================================================================
# MAGIC -- We skip 'GRANT USAGE ON CATALOG' because your version 1.0 doesn't support it.
# MAGIC -- We use 'account users' because that principal is guaranteed to exist.
# MAGIC
# MAGIC GRANT SELECT ON TABLE gold.products TO `account users`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA silver TO `account users`;
# MAGIC
# MAGIC -- ====================================================================
# MAGIC -- 4. CONTROLLED VIEW
# MAGIC -- ====================================================================
# MAGIC CREATE OR REPLACE VIEW gold.top_products AS
# MAGIC SELECT product_name, revenue, conversion_rate
# MAGIC FROM gold.products
# MAGIC WHERE purchases > 10
# MAGIC ORDER BY revenue DESC 
# MAGIC LIMIT 100;
# MAGIC
# MAGIC -- Grant access to the view specifically
# MAGIC GRANT SELECT ON VIEW gold.top_products TO `account users`;