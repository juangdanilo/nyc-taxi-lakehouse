# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — raw ingestion
# MAGIC Land the source exactly as it arrives, plus lineage columns. No cleaning here:
# MAGIC bronze is the replayable record of what we received.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

batch_id = get_batch_id()

raw = (
    spark.table(SOURCE)
    .where(BATCHES[batch_id])
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
)

# Append-only: re-running a batch duplicates rows here on purpose; silver dedupes them.
raw.write.format("delta").mode("append").saveAsTable(BRONZE)

# COMMAND ----------

display(spark.sql(f"SELECT _batch_id, count(*) AS rows FROM {BRONZE} GROUP BY _batch_id ORDER BY _batch_id"))
