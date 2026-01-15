# Databricks notebook source
# DBTITLE 1,Cell 1
# Bronze Layer: Read from Delta table instead of CSV
bronze_source_table = "workspace.default.nov_events_table"
layer = "bronze"  # You can parameterize this if needed

print(f"Running ETL for layer: {layer} using source table: {bronze_source_table}")

from pyspark.sql.functions import current_timestamp, col, to_date, sum, count, countDistinct

def run_layer(layer_name, bronze_source_table):
    if layer_name == "bronze":
        print("Executing Bronze Layer...")
        # Create Bronze volume if not exists
        spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ecommerce.bronze")
        # Read raw event data from Delta table
        oct_df = spark.read.table(bronze_source_table)
        # Add ingestion timestamp
        bronze_df = oct_df.withColumn("ingestion_time", current_timestamp())
        # Bronze Delta path
        bronze_path = "/Volumes/workspace/ecommerce/bronze/ecommerce_events"
        # Write Bronze data
        bronze_df.write.format("delta").mode("overwrite").save(bronze_path)
        # Sanity checks
        print("Bronze row count:", bronze_df.count())
        display(bronze_df.limit(10))
        bronze_df.printSchema()
    elif layer_name == "silver":
        print("Executing Silver Layer...")
        spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ecommerce.silver")
        bronze_df = spark.read.format("delta").load("/Volumes/workspace/ecommerce/bronze/ecommerce_events")
        silver_df = (
            bronze_df
            .filter(col("user_id").isNotNull())
            .filter(col("event_type").isin("view", "cart", "purchase"))
            .filter((col("price").isNull()) | (col("price") >= 0))
            .dropDuplicates()
        )
        silver_path = "/Volumes/workspace/ecommerce/silver/ecommerce_events_clean"
        silver_df.write.format("delta").mode("overwrite").save(silver_path)
        print("Bronze rows:", bronze_df.count())
        print("Silver rows:", silver_df.count())
        print("Null user_id count:", silver_df.filter(col("user_id").isNull()).count())
        silver_df.groupBy("event_type").count().show()
        print("Negative price count:", silver_df.filter(col("price") < 0).count())
        display(silver_df.limit(10))
    elif layer_name == "gold":
        print("Executing Gold Layer...")
        spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ecommerce.gold")
        silver_df = spark.read.format("delta").load("/Volumes/workspace/ecommerce/silver/ecommerce_events_clean")
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
        gold_path = "/Volumes/workspace/ecommerce/gold/daily_sales_metrics"
        gold_df.write.format("delta").mode("overwrite").save(gold_path)
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
run_layer(layer, bronze_source_table)
