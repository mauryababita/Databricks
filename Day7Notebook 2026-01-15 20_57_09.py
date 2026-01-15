# Databricks notebook source
# Databricks notebook source
# ============================================================
# MEDALLION PIPELINE NOTEBOOK (Bronze / Silver / Gold)
# Used by Databricks Jobs with task-level parameters
# ============================================================

from pyspark.sql import functions as F

# ------------------------------------------------------------
# 1. WIDGET SETUP (Works for Notebook + Jobs)
# ------------------------------------------------------------
dbutils.widgets.removeAll()

dbutils.widgets.text(
    "source_path",
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    "Source File Path"
)

dbutils.widgets.dropdown(
    "layer",
    "bronze",
    ["bronze", "silver", "gold"],
    "Select Processing Layer"
)

source_file = dbutils.widgets.get("source_path")
active_layer = dbutils.widgets.get("layer")

print(f"Starting job for layer: {active_layer}")

# ------------------------------------------------------------
# 2. BRONZE PROCESSING
# ------------------------------------------------------------
def process_bronze(source_file: str):
    print("Running Bronze Layer...")

    raw_df = (
        spark.read
        .csv(source_file, header=True, inferSchema=True)
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    raw_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("bronze_events")

    print("Bronze layer completed successfully")

# ------------------------------------------------------------
# 3. SILVER PROCESSING
# ------------------------------------------------------------
def process_silver():
    print("Running Silver Layer...")

    bronze_df = spark.read.table("bronze_events")

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

    silver_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_events")

    print("Silver layer completed successfully")

# ------------------------------------------------------------
# 4. GOLD PROCESSING
# ------------------------------------------------------------
def process_gold():
    print("Running Gold Layer...")

    silver_df = spark.read.table("silver_events")

    gold_df = (
        silver_df
        .groupBy("product_id", "product_name")
        .agg(F.sum("price").alias("total_revenue"))
    )

    gold_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("gold_product_revenue")

    print("Gold layer completed successfully")

# ------------------------------------------------------------
# 5. LAYER ROUTING (Used by Job Tasks)
# ------------------------------------------------------------
if active_layer == "bronze":
    process_bronze(source_file)

elif active_layer == "silver":
    process_silver()

elif active_layer == "gold":
    process_gold()

else:
    raise ValueError("Invalid layer selected")

print("Job finished successfully")
