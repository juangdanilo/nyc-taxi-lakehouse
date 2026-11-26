# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Delta Lake features, demonstrated
# MAGIC Runs against a small copy of silver so the real table is never touched.
# MAGIC Cells that are *expected* to fail catch the error and print it.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

DEMO = f"{CATALOG}.silver.trips_demo"
spark.sql(f"CREATE OR REPLACE TABLE {DEMO} AS SELECT * FROM {SILVER} LIMIT 1000")
spark.sql(f"ALTER TABLE {DEMO} ADD CONSTRAINT non_negative_fare CHECK (fare_amount >= 0)")
sample = spark.table(DEMO).limit(1)

# COMMAND ----------

# MAGIC %md ## 1. Schema enforcement
# MAGIC Why it matters: a bad upstream change fails loudly instead of silently corrupting the table.

# COMMAND ----------

try:
    sample.withColumn("fare_amount", F.lit("not a number")).write.mode("append").saveAsTable(DEMO)
except Exception as e:
    print("Rejected as expected:", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md ## 2. Schema evolution
# MAGIC Why it matters: additive changes are allowed when you opt in explicitly.

# COMMAND ----------

sample.withColumn("tip_amount", F.lit(2.5)).write.mode("append").option("mergeSchema", "true").saveAsTable(DEMO)
print(spark.table(DEMO).columns)

# COMMAND ----------

# MAGIC %md ## 3. CHECK constraint
# MAGIC Why it matters: data-quality rules live in the table, not only in pipeline code.

# COMMAND ----------

try:
    sample.withColumn("fare_amount", F.lit(-1.0)).withColumn("tip_amount", F.lit(0.0)).write.mode(
        "append"
    ).saveAsTable(DEMO)
except Exception as e:
    print("Rejected as expected:", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md ## 4. Time travel + RESTORE
# MAGIC Why it matters: a bad DELETE is recoverable in seconds, no backups needed.

# COMMAND ----------

good_version = spark.sql(f"DESCRIBE HISTORY {DEMO} LIMIT 1").first()["version"]
spark.sql(f"DELETE FROM {DEMO} WHERE fare_amount > 0")  # the "accident"

print("now:", spark.table(DEMO).count())
print(f"version {good_version}:", spark.sql(f"SELECT * FROM {DEMO} VERSION AS OF {good_version}").count())

spark.sql(f"RESTORE TABLE {DEMO} TO VERSION AS OF {good_version}")
print("after restore:", spark.table(DEMO).count())

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {DEMO}").select("version", "operation", "operationParameters"))
