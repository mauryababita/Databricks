# Databricks notebook source
events.printSchema()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Load the data
events = spark.table("default.silver_events")

# 2. Descriptive stats
events.describe(["price"]).show()

# 3. Weekend Conversion (Fixed event_time)
weekday_df = events.withColumn("is_weekend", F.dayofweek("event_time").isin([1, 7]))
weekday_df.groupBy("is_weekend", "event_type").count().show()

# 4. Feature Engineering
# --- CRITICAL: You must define the window BEFORE using it in 'features' ---
user_window = Window.partitionBy("user_id").orderBy("event_time")

features = events.withColumn("hour", F.hour("event_time")) \
    .withColumn("day_of_week", F.dayofweek("event_time")) \
    .withColumn("price_log", F.log(F.col("price") + 1)) \
    .withColumn("time_since_first_view",
        F.unix_timestamp("event_time") - 
        F.first(F.unix_timestamp("event_time")).over(user_window))

features.select("user_id", "event_time", "price_log", "time_since_first_view").show(5)

# 5. Correlation (Optional Check)
try:
    corr_val = events.stat.corr("price", "conversion_rate")
    print(f"Correlation: {corr_val}")
except Exception:
    print("Column 'conversion_rate' not found for correlation.")

# COMMAND ----------

