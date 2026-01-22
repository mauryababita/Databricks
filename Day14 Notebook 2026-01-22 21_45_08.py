# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %pip install transformers xformers torch

# COMMAND ----------

# MAGIC %pip install transformers xformers torch torchvision mlflow

# COMMAND ----------

import mlflow
from transformers import pipeline

# 1. Enable Autologging (This captures model metadata automatically)
mlflow.transformers.autolog()

# 2. Initialize the pipeline
classifier = pipeline("sentiment-analysis")

reviews = ["This product is amazing!", "Terrible quality, waste of money"]
results = classifier(reviews)

# 3. Log to MLflow
with mlflow.start_run(run_name="sentiment_analysis_v1"):
    # Perform the classification
    for review, result in zip(reviews, results):
        print(f"Review: {review} | Result: {result['label']}")

    # Manual metrics if you have them
    mlflow.log_metric("accuracy_score", 0.98) 

print("\nModel and dependencies are now logged in the MLflow Experiment UI.")