# Databricks notebook source
# Databricks notebook source
# ============================================================
# MEDALLION ETL PIPELINE (BRONZE → SILVER → GOLD)
# Used by Databricks Jobs (Multi-Task Workflow)
# ============================================================

from pyspark.sql.functions import (
    current_timestamp, col, to_date,
    sum, count, countDistinct
)

# ------------------------------------------------------------
# 1. PARAMETER WIDGETS (JOB + NOTEBOOK SAFE)
# ------------------------------------------------------------
dbutils.widgets.removeAll()

dbutils.widgets.dropdown(
    "layer",
    "bronze",
    ["bronze", "silver", "gold"],
    "Processing Layer"
)

layer = dbutils.widgets.get("layer")

# Source Delta table for Bronze layer
BRONZE_SOURCE_TABLE = "workspace.default.nov_events_table"

# Volume paths
BRONZE_PATH = "/Volumes/workspace/ecommerce/bronze/ecommerce_events"
SILVER_PATH = "/Volumes/workspace/ecommerce/silver/ecommerce_events_clean"
GOLD_PATH   = "/Volumes/workspace/ecommerce/gold/daily_sales_metrics"

print(f"Running ETL for layer: {layer}")
print(f"Bronze source table: {BRONZE_SOURCE_TABLE}")

# ------------------------------------------------------------
# 2. BRONZE LAYER
# ------------------------------------------------------------
def run_bronze():
    try:
        print("Executing Bronze Layer...")

        spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ecommerce.bronze")

        bronze_df = (
            spark.read.table(BRONZE_SOURCE_TABLE)
            .withColumn("ingestion_time", current_timestamp())
        )

        bronze_df.write.format("delta").mode("overwrite").save(BRONZE_PATH)

        print("Bronze ingestion completed")
        print("Bronze row count:", bronze_df.count())

    except Exception as e:
        raise RuntimeError(f"Bronze layer failed: {e}")

# ------------------------------------------------------------
# 3. SILVER LAYER
# ------------------------------------------------------------
def run_silver():
    try:
        print("Executing Silver Layer...")

        spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ecommerce.silver")

        bronze_df = spark.read.format("delta").load(BRONZE_PATH)

        silver_df = (
            bronze_df
            .filter(col("user_id").isNotNull())
            .filter(col("event_type").isin("view", "cart", "purchase"))
            .filter((col("price").isNull()) | (col("price") >= 0))
            .dropDuplicates()
        )

        silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)

        print("Silver cleansing completed")
        print("Bronze rows:", bronze_df.count())
        print("Silver rows:", silver_df.count())

    except Exception as e:
        raise RuntimeError(f"Silver layer failed: {e}")

# ------------------------------------------------------------
# 4. GOLD LAYER
# ------------------------------------------------------------
def run_gold():
    try:
        print("Executing Gold Layer...")

        spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ecommerce.gold")

        silver_df = spark.read.format("delta").load(SILVER_PATH)

        gold_df = (
            silver_df
            .filter(col("event_type") == "purchase")
            .withColumn("event_date", to_date("event_time"))
            .groupBy("event_date")
            .agg(
                sum("price").alias("total_revenue"),
                count("*").alias("total_orders"),
                countDistinct("user_id").alias("unique_customers")
            )
        )

        gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

        print("Gold aggregation completed")
        print("Gold row count:", gold_df.count())

    except Exception as e:
        raise RuntimeError(f"Gold layer failed: {e}")

# ------------------------------------------------------------
# 5. LAYER ROUTING (USED BY JOB TASKS)
# ------------------------------------------------------------
if layer == "bronze":
    run_bronze()
elif layer == "silver":
    run_silver()
elif layer == "gold":
    run_gold()
else:
    raise ValueError(f"Invalid layer parameter: {layer}")

print("ETL job completed successfully")


# COMMAND ----------


dbutils.widgets.text("source_path", "/Volumes/workspace/ecommerce", "Source Path")
dbutils.widgets.dropdown("layer", "bronze", ["bronze","silver","gold"], "Layer")

# Read parameter values
source_path = dbutils.widgets.get("source_path")
layer = dbutils.widgets.get("layer")

print(f"Running ETL for layer: {layer} using source path: {source_path}")

from pyspark.sql.functions import current_timestamp, col, to_date, sum, count, countDistinct

def run_layer(layer_name, source_path):
    
    if layer_name == "bronze":
        print("Executing Bronze Layer...")
        
        # Create Bronze volume if not exists (SQL cell)
        spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ecommerce.bronze")
        
        # Read raw CSV data
        oct_df = spark.read.csv(
            f"{source_path}/ecommerce_data/2019-Oct.csv",
            header=True,
            inferSchema=True
        )
        
        # Add ingestion timestamp
        bronze_df = oct_df.withColumn("ingestion_time", current_timestamp())
        
        # Bronze Delta path
        bronze_path = f"{source_path}/bronze/ecommerce_events"
        
        # Write Bronze data
        bronze_df.write.format("delta").mode("overwrite").save(bronze_path)
        
        # Sanity checks
        print("Bronze row count:", bronze_df.count())
        display(bronze_df.limit(10))
        bronze_df.printSchema()
    
    elif layer_name == "silver":
        print("Executing Silver Layer...")
        
        # Create Silver volume
        spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ecommerce.silver")
        
        # Read Bronze data
        bronze_df = spark.read.format("delta").load(f"{source_path}/bronze/ecommerce_events")
        
        # Clean and validate
        silver_df = (
            bronze_df
            .filter(col("user_id").isNotNull())
            .filter(col("event_type").isin("view", "cart", "purchase"))
            .filter((col("price").isNull()) | (col("price") >= 0))
            .dropDuplicates()
        )
        
        # Silver Delta path
        silver_path = f"{source_path}/silver/ecommerce_events_clean"
        
        # Write Silver data
        silver_df.write.format("delta").mode("overwrite").save(silver_path)
        
        # Checks
        print("Bronze rows:", bronze_df.count())
        print("Silver rows:", silver_df.count())
        print("Null user_id count:", silver_df.filter(col("user_id").isNull()).count())
        silver_df.groupBy("event_type").count().show()
        print("Negative price count:", silver_df.filter(col("price") < 0).count())
        display(silver_df.limit(10))
    
    elif layer_name == "gold":
        print("Executing Gold Layer...")
        
        # Create Gold volume
        spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ecommerce.gold")
        
        # Read Silver data
        silver_df = spark.read.format("delta").load(f"{source_path}/silver/ecommerce_events_clean")
        
        # Aggregates for analytics
        gold_df = (
            silver_df
            .filter(col("event_type") == "purchase")
            .withColumn("event_date", to_date("event_time"))
            .groupBy("event_date")
            .agg(
                sum("price").alias("total_revenue"),
                count("*").alias("total_orders"),
                countDistinct("user_id").alias("unique_customers")
            )
        )
        
        # Gold Delta path
        gold_path = f"{source_path}/gold/daily_sales_metrics"
        
        # Write Gold data
        gold_df.write.format("delta").mode("overwrite").save(gold_path)
        
        # Checks
        display(gold_df.orderBy("event_date").limit(10))
        print("Duplicate dates:", gold_df.count() - gold_df.select("event_date").distinct().count())
        gold_df.select(
            "event_date",
            "total_revenue",
            "total_orders",
            "unique_customers"
        ).summary().show()
    
    else:
        raise ValueError(f"Unknown layer: {layer_name}")
    # Execute the layers
run_layer(layer, source_path)