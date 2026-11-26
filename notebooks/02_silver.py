# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — clean, dedupe, MERGE
# MAGIC Cleaning rules live in `src/taxi_pipeline/transforms.py` and are unit-tested locally.
# MAGIC The write is an idempotent `MERGE` on `trip_id`: running the same batch twice
# MAGIC leaves the table unchanged.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from taxi_pipeline.transforms import clean_trips, dedupe_latest, upsert_trips

dbutils.widgets.dropdown("batch_id", "1", list(BATCHES))
batch_id = dbutils.widgets.get("batch_id")

updates = dedupe_latest(clean_trips(spark.table(BRONZE).where(F.col("_batch_id") == batch_id)))

if not spark.catalog.tableExists(SILVER):
    updates.write.format("delta").saveAsTable(SILVER)
    # Data-quality guard enforced by Delta on every future write
    spark.sql(f"ALTER TABLE {SILVER} ADD CONSTRAINT non_negative_fare CHECK (fare_amount >= 0)")
else:
    upsert_trips(DeltaTable.forName(spark, SILVER), updates)

print(f"{SILVER}: {spark.table(SILVER).count():,} rows")

# COMMAND ----------

# MAGIC %md Every MERGE shows up as a new table version with its metrics.

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {SILVER}").select("version", "timestamp", "operation", "operationMetrics")
)
