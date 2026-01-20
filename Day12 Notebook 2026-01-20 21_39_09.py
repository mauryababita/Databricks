# Databricks notebook source
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from pyspark.sql import functions as F

# 1. Prepare data by aggregating silver_events into product-level metrics
# This creates the "Gold" view that was missing
events = spark.table("default.silver_events")

product_stats = events.groupBy("brand").agg(
    F.count(F.when(F.col("event_type") == "view", 1)).alias("views"),
    F.count(F.when(F.col("event_type") == "cart", 1)).alias("cart_adds"),
    F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases")
)

# Convert to Pandas for sklearn
df = product_stats.toPandas()

# Ensure we have data before splitting
if df.empty:
    print("Error: The resulting DataFrame is empty. Check your silver_events data.")
else:
    X = df[["views", "cart_adds"]]
    y = df["purchases"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 2. MLflow experiment
    with mlflow.start_run(run_name="linear_regression_v1"):
        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_param("test_size", 0.2)

        model = LinearRegression()
        model.fit(X_train, y_train)

        score = model.score(X_test, y_test)
        mlflow.log_metric("r2_score", score)

        # Log model
        mlflow.sklearn.log_model(model, "model")

    print(f"R² Score: {score:.4f}")