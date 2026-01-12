# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load the October dataset
events = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv",
    header=True,
    inferSchema=True
)

# Derive product_name from category_code
# Using r"\." fixes the SyntaxWarning saw earlier
events = events.withColumn(
    "product_name",
    F.coalesce(F.element_at(F.split(F.col("category_code"), r"\."), -1), F.lit("Other"))
)

# COMMAND ----------

# --- Top 5 products by revenue --- # Filters for 'purchase' events and sums the 'price'
revenue = events.filter(F.col("event_type") == "purchase") \
    .groupBy("product_id", "product_name") \
    .agg(F.sum("price").alias("revenue")) \
    .orderBy(F.desc("revenue")).limit(5)

display(revenue)

# --- Running total per user ---
# Partition by user_id and order by event_time as per schema
window = Window.partitionBy("user_id").orderBy("event_time")
events_with_totals = events.withColumn("cumulative_events", F.count("*").over(window))

display(events_with_totals.select("user_id", "event_time", "cumulative_events").limit(10))

# COMMAND ----------

# 2. Running total revenue per user
# Partitioning by user_id (long) and ordering by event_time (timestamp)
window_spec = Window.partitionBy("user_id").orderBy("event_time")

events_with_running_total = events.withColumn(
    "running_total_spend",
    F.sum("price").over(window_spec)
)

display(events_with_running_total.select("user_id", "event_time", "price", "running_total_spend").limit(5))

# COMMAND ----------

# 3. Conversion rate by category
conversion_df = events.filter(F.col("category_code").isNotNull()) \
    .groupBy("category_code") \
    .pivot("event_type") \
    .count()

# Add the calculation and handle potential division by zero using coalesce
report = conversion_df.withColumn(
    "conversion_rate",
    F.round((F.col("purchase") / F.col("view")) * 100, 2)
).orderBy(F.desc("conversion_rate"))

display(report.limit(5))

# COMMAND ----------

# Create Product Dimension (Cell 7)
products = events.select("product_id", "category_code", "brand").distinct()

# Create User Dimension (Cell 8)
users = events.select("user_id").distinct()

# COMMAND ----------

# Inner Join
events_products = events.join(products, on="product_id", how="inner")
display(events_products.limit(5))

# Left Join
events_users = events.join(users, on="user_id", how="left")
display(events_users.limit(5))

# Full Outer Join
full_join = events.join(products, "product_id", "outer")
display(full_join.limit(5))

# COMMAND ----------

