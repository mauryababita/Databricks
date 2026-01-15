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