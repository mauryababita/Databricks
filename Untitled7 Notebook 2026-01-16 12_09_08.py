# Databricks notebook source
# Widgets
dbutils.widgets.text(
    "source_path",
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    "Source File Path"
)

dbutils.widgets.dropdown(
    "layer",
    "bronze",
    ["bronze", "silver", "gold"],
    "Layer"
)

source_path = dbutils.widgets.get("source_path")
layer = dbutils.widgets.get("layer")


# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("source_path", "/default/path")

source_path = dbutils.widgets.get("source_path")

df = spark.read.csv(source_path, header=True, inferSchema=True)

(
    df.withColumn("ingestion_ts", F.current_timestamp())
      .write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("default.bronze_events")
)

dbutils.notebook.exit("Bronze completed")


# COMMAND ----------

from pyspark.sql import functions as F

if not spark.catalog.tableExists("default.bronze_events"):
    raise RuntimeError("Bronze table missing")

bronze_df = spark.read.table("default.bronze_events")

silver_df = (
    bronze_df
    .filter(F.col("price").isNotNull())
    .filter(F.col("price") > 0)
    .dropDuplicates(["user_session", "event_time"])
    .withColumn(
        "product_name",
        F.coalesce(
            F.element_at(F.split("category_code", r"\."), -1),
            F.lit("Other")
        )
    )
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("default.silver_events")
)

dbutils.notebook.exit("Silver completed")


# COMMAND ----------

from pyspark.sql import functions as F

if not spark.catalog.tableExists("default.silver_events"):
    raise RuntimeError("Silver table missing")

silver_df = spark.read.table("default.silver_events")

gold_df = (
    silver_df
    .groupBy("product_id", "product_name")
    .agg(F.sum("price").alias("total_revenue"))
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("default.gold_product_revenue")
)

dbutils.notebook.exit("Gold completed")
