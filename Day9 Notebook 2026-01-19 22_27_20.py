# Databricks notebook source
# Databricks notebook source
# MAGIC %sql
# MAGIC -- Query 1: Revenue Trends Over Time
# MAGIC SELECT
# MAGIC   event_date,
# MAGIC   COALESCE(SUM(daily_revenue), 0) AS total_revenue,
# MAGIC   COALESCE(SUM(daily_purchases), 0) AS total_purchases,
# MAGIC   COALESCE(SUM(daily_views), 0) AS total_views,
# MAGIC   CASE
# MAGIC     WHEN SUM(daily_views) > 0
# MAGIC     THEN ROUND(SUM(daily_purchases) * 100.0 / SUM(daily_views), 2)
# MAGIC     ELSE 0
# MAGIC   END AS conversion_rate
# MAGIC FROM gold_daily_metrics_all
# MAGIC GROUP BY event_date
# MAGIC ORDER BY event_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 2: Funnel Analysis (View → Cart → Purchase)
# MAGIC
# MAGIC -- Step 0: Combine October + November Silver tables
# MAGIC WITH combined_silver AS (
# MAGIC     SELECT event_date, user_id, event_type FROM default.silver_events_oct
# MAGIC     UNION ALL
# MAGIC     SELECT event_date, user_id, event_type FROM default.silver_df_nov_realworld
# MAGIC ),
# MAGIC
# MAGIC -- Step 1: Aggregate by event type to get unique users per stage
# MAGIC funnel_data AS (
# MAGIC     SELECT
# MAGIC         event_type AS step,
# MAGIC         COUNT(DISTINCT user_id) AS value
# MAGIC     FROM combined_silver
# MAGIC     GROUP BY event_type
# MAGIC )
# MAGIC
# MAGIC -- Step 2: Optional conversion percentage
# MAGIC SELECT
# MAGIC     step,
# MAGIC     value,
# MAGIC     ROUND(
# MAGIC         CASE 
# MAGIC             WHEN step = 'purchase' THEN 
# MAGIC                  100.0 * value / (SELECT value FROM funnel_data WHERE step='view')
# MAGIC             ELSE NULL
# MAGIC         END, 2
# MAGIC     ) AS conversion_rate
# MAGIC FROM funnel_data
# MAGIC ORDER BY 
# MAGIC     CASE step
# MAGIC         WHEN 'view' THEN 1
# MAGIC         WHEN 'cart' THEN 2
# MAGIC         WHEN 'purchase' THEN 3
# MAGIC     END;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 3: Top Products by Revenue
# MAGIC -- Top Products Combined (Oct + Nov)
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   brand,
# MAGIC   SUM(revenue) AS total_revenue,
# MAGIC   AVG(conversion_rate) AS avg_conversion_rate
# MAGIC FROM gold_product_metrics_all
# MAGIC GROUP BY product_id, brand
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------