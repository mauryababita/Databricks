# Databricks notebook source
# Databricks notebook source
# 1. Create the Job
# Click Jobs & Pipelines in the sidebar -> Create Job.

# Task 1 (Bronze):
# Name: Bronze_Ingestion
# Type: Notebook
# Parameters: Click Add, Key: layer, Value: bronze.
#             Key: source_path, Value: (/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv).

# Task 2 (Silver):
# Click the + icon below Task 1.
# Name: Silver_Cleaning
# Depends on: Bronze_Ingestion
# Parameters: Key: layer, Value: silver.

# Task 3 (Gold):
# Click the + icon below Task 2.
# Name: Gold_Aggregates
# Depends on: Silver_Cleaning
# Parameters: Key: layer, Value: gold.

# 2. Set Dependency (Task 3)
# By setting the "Depends on" field, you ensure that Silver won't start until Bronze is finished.
# This is called a Directed Acyclic Graph (DAG).

# 3. Schedule Execution (Task 4)
# On the right panel of the Job UI, look for Schedule.


# COMMAND ----------

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

# 1. Setup Widgets (Clean up existing ones first)
dbutils.widgets.removeAll()

# 2. Create a text widget for the file path
dbutils.widgets.text(
    "source_path",
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    "Source File Path"
)

# 3. Create a dropdown to select which layer to process
dbutils.widgets.dropdown(
    "layer",
    "bronze",
    ["bronze", "silver", "gold"],
    "Select Processing Layer"
)

# 4. Get the values from the UI
source_file = dbutils.widgets.get("source_path")
active_layer = dbutils.widgets.get("layer")

print(f"Job started for Layer: {active_layer}")
print(f"Processing Source: {source_file}")


# COMMAND ----------

