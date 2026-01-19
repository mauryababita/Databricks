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

# COMMAND ----------

# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze.events;
# MAGIC
# MAGIC CREATE TABLE bronze.events AS
# MAGIC SELECT * FROM workspace.default.bronze_df_nov;
# MAGIC
# MAGIC SELECT COUNT(*) FROM bronze.events;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver.events_clean;
# MAGIC
# MAGIC CREATE TABLE silver.events_clean AS
# MAGIC SELECT * FROM workspace.default.silver_df_nov_realworld;
# MAGIC
# MAGIC SELECT COUNT(*) FROM silver.events_clean;

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold.product_metrics;
# MAGIC DROP TABLE IF EXISTS gold.category_metrics;
# MAGIC DROP TABLE IF EXISTS gold.daily_metrics;
# MAGIC
# MAGIC CREATE TABLE gold.product_metrics AS
# MAGIC SELECT * FROM workspace.default.gold_df_product_nov;
# MAGIC
# MAGIC CREATE TABLE gold.category_metrics AS
# MAGIC SELECT * FROM workspace.default.gold_df_category_nov;
# MAGIC
# MAGIC CREATE TABLE gold.daily_metrics AS
# MAGIC SELECT * FROM workspace.default.gold_df_daily_nov;
# MAGIC
# MAGIC SHOW TABLES IN gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.top_products AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   brand,
# MAGIC   revenue,
# MAGIC   conversion_rate
# MAGIC FROM gold.product_metrics
# MAGIC WHERE revenue > 0
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 100;
# MAGIC
# MAGIC SELECT * FROM gold.top_products;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED gold.top_products;

# COMMAND ----------