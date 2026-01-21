# Databricks notebook source
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from pyspark.sql import functions as F

# 1. DATA PREPARATION (Fixes the NameError)
# We aggregate the silver_events into features we can actually use
events = spark.table("default.silver_events")
product_stats = events.groupBy("brand").agg(
    F.count(F.when(F.col("event_type") == "view", 1)).alias("views"),
    F.count(F.when(F.col("event_type") == "cart", 1)).alias("cart_adds"),
    F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases")
)

# Convert to Pandas for Scikit-Learn
df = product_stats.toPandas()
X = df[["views", "cart_adds"]]
y = df["purchases"]

# Define the missing variables
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 2. SCIKIT-LEARN MODELS
models = {
    "linear": LinearRegression(),
    "decision_tree": DecisionTreeRegressor(max_depth=5),
    "random_forest": RandomForestRegressor(n_estimators=100)
}

for name, model in models.items():
    with mlflow.start_run(run_name=f"{name}_model"):
        mlflow.log_param("model_type", name)
        
        # Now X_train and y_train are defined!
        model.fit(X_train, y_train)
        score = model.score(X_test, y_test)

        mlflow.log_metric("r2_score", score)
        mlflow.sklearn.log_model(model, "model")

        print(f"sklearn {name}: R² = {score:.4f}")