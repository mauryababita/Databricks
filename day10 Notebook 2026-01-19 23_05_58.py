# Databricks notebook source
display(spark.sql("SHOW TABLES IN default"))

# COMMAND ----------

# 1. Explain Query (Ensure we point to default schema)
spark.sql("SELECT * FROM default.silver_events WHERE event_type='purchase'").explain(True)

# 2. Create Partitioned Table
# Using IF NOT EXISTS to prevent errors on re-runs
spark.sql("""
  CREATE TABLE IF NOT EXISTS default.silver_events_part
  USING DELTA
  PARTITIONED BY (event_type)
  AS SELECT * FROM default.silver_events
""")

# 3. Optimize for Serverless Performance
# Instead of .cache(), we use ZORDER to cluster data physically
print("Optimizing table layout...")
spark.sql("OPTIMIZE default.silver_events_part ZORDER BY (user_id, product_id)")

# 4. Benchmark
import time
start = time.time()
# Filtering on a Z-Ordered column (user_id) will be extremely fast
row_count = spark.sql("SELECT * FROM default.silver_events_part WHERE user_id=12345").count()
duration = time.time() - start

print(f"Benchmark Result: {row_count} rows found")
print(f"Time: {duration:.2f}s")

# 5. Iterative Query Handling
# On Serverless, just reference the table. 
# The Delta Engine will automatically use its internal 'Disk Cache'.
df_final = spark.table("default.silver_events_part")