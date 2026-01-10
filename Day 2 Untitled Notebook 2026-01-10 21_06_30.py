# Databricks notebook source
# Load data
events = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv",
    header=True,
    inferSchema=True
)

# Basic operations
display(
    events.select(
        "event_type",
        "price",
        "brand"
    ).limit(10)
)
events.filter("price > 100").count()
display(
    events.groupBy("event_type").count()
)
top_brands = events.groupBy("brand").count().orderBy("count", ascending=False).limit(5)