# Databricks notebook source
dbutils.widgets.text("run_date", "2026-01-15")
dbutils.widgets.text("layer", "bronze")


# COMMAND ----------

run_date = dbutils.widgets.get("run_date")

try:
    df = (
        spark.read
        .option("header", True)
        .csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv")
    )

    df.write.mode("append").saveAsTable("workspace.ecommerce.bronze_events")

    print(f"Bronze processing completed for run_date={run_date}")

except Exception as e:
    raise RuntimeError(f"Bronze layer failed: {str(e)}")

# COMMAND ----------

dbutils.widgets.text("run_date", "2026-01-15")
dbutils.widgets.text("layer", "silver")


# COMMAND ----------

run_date = dbutils.widgets.get("run_date")

try:
    bronze_df = spark.table("workspace.ecommerce.bronze_events")

    silver_df = bronze_df.dropDuplicates()

    silver_df.write.mode("overwrite").saveAsTable(
        "workspace.ecommerce.silver_events"
    )

    print(f"Silver processing completed for run_date={run_date}")

except Exception as e:
    raise RuntimeError(f"Silver layer failed: {str(e)}")

# COMMAND ----------

dbutils.widgets.text("run_date", "2026-01-15")
dbutils.widgets.text("layer", "gold")

# COMMAND ----------

run_date = dbutils.widgets.get("run_date")

try:
    silver_df = spark.table("workspace.ecommerce.silver_events")

    gold_df = silver_df.groupBy("event_type").count()

    gold_df.write.mode("overwrite").saveAsTable(
        "workspace.ecommerce.gold_events"
    )

    print(f"Gold processing completed for run_date={run_date}")

except Exception as e:
    raise RuntimeError(f"Gold layer failed: {str(e)}")

# COMMAND ----------

# Databricks notebook source

from pyspark.sql import functions as F

def process_bronze():
    print("Running Bronze Layer...")
    raw_df = spark.read.csv(source_file, header=True, inferSchema=True)
    raw_df.withColumn(
        "ingestion_ts", F.current_timestamp()
    ).write.format("delta").mode("overwrite").saveAsTable("bronze_events")
    return "Bronze Success"


def process_silver():
    print("Running Silver Layer...")
    bronze_df = spark.read.table("bronze_events")

    # Using the cleaning logic from Day 6
    silver_df = (
        bronze_df
        .filter(F.col("price") > 0)
        .dropDuplicates(["user_session", "event_time"])
        .withColumn(
            "product_name",
            F.coalesce(
                F.element_at(F.split(F.col("category_code"), r"\."), -1),
                F.lit("Other")
            )
        )
    )

    silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_events")
    return "Silver Success"


def process_gold():
    print("Running Gold Layer...")
    silver_df = spark.read.table("silver_events")

    gold_df = (
        silver_df
        .groupBy("product_id", "product_name")
        .agg(F.sum("price").alias("total_revenue"))
    )

    gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_product_revenue")
    return "Gold Success"


# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    "Source File Path"
)

# COMMAND ----------

dbutils.widgets.dropdown(
    "layer",
    "bronze",
    ["bronze", "silver", "gold"],
    "Select Processing Layer"
)

# COMMAND ----------

source_file = dbutils.widgets.get("source_path")
active_layer = dbutils.widgets.get("layer")

print(f"Job started for Layer: {active_layer}")
print(f"Processing Source: {source_file}")


# COMMAND ----------