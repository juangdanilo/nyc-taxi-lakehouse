# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — business aggregates
# MAGIC Demand and pricing per pickup zip and hour. Rebuilt with `overwrite` because it is
# MAGIC small and fully derived from silver: simpler than incremental and always consistent.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from taxi_pipeline.transforms import trips_by_zip_hour

trips_by_zip_hour(spark.table(SILVER)).write.format("delta").mode("overwrite").saveAsTable(GOLD)

# COMMAND ----------

# MAGIC %md
# MAGIC Dashboards filter by zip, so co-locate rows by `pickup_zip` to skip files on read.
# MAGIC (On serverless, if ZORDER is rejected, use `ALTER TABLE ... CLUSTER BY (pickup_zip)` instead.)

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {GOLD} ZORDER BY (pickup_zip)"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {GOLD} ORDER BY trips DESC LIMIT 20"))
