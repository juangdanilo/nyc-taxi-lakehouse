"""Pure transformation logic for the NYC taxi lakehouse.

Every function takes and returns DataFrames (or a DeltaTable) so it can be
unit-tested locally without Databricks.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Columns that together identify a trip; the source has no natural key.
KEY_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pickup_zip",
    "dropoff_zip",
    "trip_distance",
    "fare_amount",
]


def clean_trips(df: DataFrame) -> DataFrame:
    """Drop invalid rows, add trip duration and a deterministic trip_id."""
    pickup = F.col("tpep_pickup_datetime").cast("timestamp")
    dropoff = F.col("tpep_dropoff_datetime").cast("timestamp")
    return (
        df.withColumn("tpep_pickup_datetime", pickup)
        .withColumn("tpep_dropoff_datetime", dropoff)
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .where(F.col("tpep_pickup_datetime").isNotNull() & F.col("tpep_dropoff_datetime").isNotNull())
        .where((F.col("fare_amount") >= 0) & (F.col("trip_distance") > 0))
        .withColumn(
            "trip_duration_min",
            F.round(
                (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60, 2
            ),
        )
        .withColumn(
            "trip_id",
            # coalesce so a null in one column cannot shift values and collide with another row
            F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in KEY_COLUMNS]), 256),
        )
    )


def dedupe_latest(df: DataFrame) -> DataFrame:
    """Keep one row per trip_id: the most recently ingested one."""
    w = Window.partitionBy("trip_id").orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def upsert_trips(target: DeltaTable, updates: DataFrame) -> None:
    """Idempotent MERGE on trip_id. `updates` must already be deduplicated."""
    (
        target.alias("t")
        .merge(updates.alias("s"), "t.trip_id = s.trip_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def trips_by_zip_hour(df: DataFrame) -> DataFrame:
    """Gold aggregate: demand and pricing per pickup zip and hour of day."""
    return (
        df.groupBy("pickup_zip", F.hour("tpep_pickup_datetime").alias("pickup_hour"))
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        )
    )
