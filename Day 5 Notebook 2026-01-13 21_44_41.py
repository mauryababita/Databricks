# Databricks notebook source
# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# 1. Load your existing Delta Table
# Using 'forName' is safer if you registered the table in the catalog on Day 4
deltaTable = DeltaTable.forName(spark, "nov_events_table")

# 2. Simulate new incremental data (taking 100 rows from the CSV)
new_data = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv", header=True, inferSchema=True).limit(100)

# 3. Apply the same product_name derivation to keep the schema consistent
updates = new_data.withColumn(
    "product_name",
    F.coalesce(F.element_at(F.split(F.col("category_code"), r"\."), -1), F.lit("Other"))
)

# 4. Perform the MERGE
deltaTable.alias("target").merge(
    updates.alias("source"),
    "target.user_session = source.user_session AND target.event_time = source.event_time"
).whenMatchedUpdateAll() \
.whenNotMatchedInsertAll() \
.execute()

print("Incremental MERGE completed successfully.")

# COMMAND ----------

# Check the history to see the 'MERGE' operation metrics
history_df = deltaTable.history().select("version", "timestamp", "operation", "operationMetrics")
display(history_df.limit(5))

# COMMAND ----------

# View the history of changes to see version numbers
display(deltaTable.history())

# Query a specific version (e.g., the state before the merge)
v0 = spark.read.format("delta").option("versionAsOf", 0).table("nov_events_table")

# Query by timestamp (Note: ensure the date format is YYYY-MM-DD HH:MM:SS)
try:
    historical_state = spark.read.format("delta") \
        .option("timestampAsOf", "2026-01-12 12:00:00") \
        .table("nov_events_table")
    display(historical_state.limit(5))
except Exception as e:
    print(f"Timestamp query failed: {e}")

# COMMAND ----------

# Show current version count
print(f"Current Row Count: {spark.table('nov_events_table').count():,}")

# Show Version 0 count (before the merge)
v0_df = spark.read.format("delta").option("versionAsOf", 0).table("nov_events_table")
print(f"Version 0 Row Count: {v0_df.count():,}")

# Display the actual data from the past
display(v0_df.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ZORDER by columns you filter on most frequently, like event_type or user_id
# MAGIC OPTIMIZE nov_events_table
# MAGIC ZORDER BY (event_type, user_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clean up files older than 7 days (168 hours)
# MAGIC VACUUM nov_events_table RETAIN 168 HOURS;

# COMMAND ----------

