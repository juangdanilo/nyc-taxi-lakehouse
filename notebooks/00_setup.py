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


def get_batch_id() -> str:
    """Read the batch_id widget/job parameter and fail fast on unknown values."""
    dbutils.widgets.dropdown("batch_id", "1", list(BATCHES))
    batch_id = dbutils.widgets.get("batch_id")
    if batch_id not in BATCHES:
        raise ValueError(f"batch_id must be one of {list(BATCHES)}, got {batch_id!r}")
    return batch_id


for layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer}")
