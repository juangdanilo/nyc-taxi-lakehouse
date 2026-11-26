# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Shared config, imported by every other notebook with `%run ./00_setup`.
# MAGIC Creates one schema per medallion layer.

# COMMAND ----------

import os
import sys

# Git folder layout: notebooks/ sits next to src/
sys.path.append(os.path.abspath("../src"))

dbutils.widgets.text("catalog", "workspace")  # Free Edition default catalog
CATALOG = dbutils.widgets.get("catalog")

BRONZE = f"{CATALOG}.bronze.trips"
SILVER = f"{CATALOG}.silver.trips"
GOLD = f"{CATALOG}.gold.trips_by_zip_hour"
SOURCE = "samples.nyctaxi.trips"

# Two batches split by pickup date, to simulate incremental loads
BATCHES = {
    "1": "tpep_pickup_datetime < '2016-02-01'",
    "2": "tpep_pickup_datetime >= '2016-02-01'",
}

for layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer}")
