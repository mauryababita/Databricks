# Databricks notebook source
# Databricks notebook source
nov_events = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    header=True,
    inferSchema=True
)

nov_events.printSchema()
nov_events.show(5)

# COMMAND ----------

nov_events.write.format("delta").mode("overwrite").save("/Volumes/workspace/ecommerce/ecommerce_data/delta/nov_events")

# COMMAND ----------

nov_events.write.format("delta").mode("overwrite").saveAsTable("nov_events_table")

# COMMAND ----------

spark.sql("""
CREATE TABLE nov_events_delta
USING DELTA
AS SELECT * FROM nov_events_table
""")

# COMMAND ----------

sample = nov_events.limit(100)
sample.write.format("delta").mode("append").save("/Volumes/workspace/ecommerce/ecommerce_data/delta/nov_events")

# COMMAND ----------

from pyspark.sql import Row

try:
    wrong_schema = spark.createDataFrame([("a","b","c")], ["x","y","z"])
    wrong_schema.write.format("delta").mode("append").save("/Volumes/workspace/ecommerce/ecommerce_data/delta/nov_events")
except Exception as e:
    print("Schema enforcement working:", e)

# COMMAND ----------

# Create a temporary view named 'nov_src' from your existing DataFrame
nov_events.createOrReplaceTempView("nov_src")

# COMMAND ----------

deduped_nov_src = (
    spark.table("nov_src")
    .dropDuplicates(
        [
            "user_id",
            "event_time"
        ]
    )
)

deduped_nov_src.createOrReplaceTempView("nov_src_deduped")

spark.sql(
    """
    MERGE INTO nov_events_delta AS target
    USING nov_src_deduped AS source
    ON target.user_id = source.user_id
        AND target.event_time = source.event_time
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------



# COMMAND ----------

# Databricks notebook source
nov_events = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    header=True,
    inferSchema=True
)

nov_events.printSchema()
nov_events.show(5)

# COMMAND ----------

nov_events.write.format("delta").mode("overwrite").save("/Volumes/workspace/ecommerce/ecommerce_data/delta/nov_events")

# COMMAND ----------

nov_events.write.format("delta").mode("overwrite").saveAsTable("nov_events_table")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE nov_events_delta
USING DELTA
AS SELECT * FROM nov_events_table
""")

# COMMAND ----------

sample = nov_events.limit(100)
sample.write.format("delta").mode("append").save("/Volumes/workspace/ecommerce/ecommerce_data/delta/nov_events")

# COMMAND ----------

from pyspark.sql import Row

try:
    wrong_schema = spark.createDataFrame([("a","b","c")], ["x","y","z"])
    wrong_schema.write.format("delta").mode("append").save("/Volumes/workspace/ecommerce/ecommerce_data/delta/nov_events")
except Exception as e:
    print("Schema enforcement working:", e)

# COMMAND ----------

# Create a temporary view named 'nov_src' from your existing DataFrame
nov_events.createOrReplaceTempView("nov_src")

# COMMAND ----------

deduped_nov_src = (
    spark.table("nov_src")
    .dropDuplicates(
        [
            "user_id",
            "event_time"
        ]
    )
)

deduped_nov_src.createOrReplaceTempView("nov_src_deduped")

spark.sql(
    """
    MERGE INTO nov_events_delta AS target
    USING nov_src_deduped AS source
    ON target.user_id = source.user_id
        AND target.event_time = source.event_time
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------

