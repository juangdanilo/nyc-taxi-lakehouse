from datetime import datetime

from delta.tables import DeltaTable

from taxi_pipeline.transforms import clean_trips, dedupe_latest, trips_by_zip_hour, upsert_trips

COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip"]
T0 = datetime(2016, 1, 1, 8, 0)
T1 = datetime(2016, 1, 1, 8, 30)


def raw(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_clean_trips_drops_invalid_rows_and_computes_duration(spark):
    df = raw(
        spark,
        [
            (T0, T1, 2.0, 10.0, 10001, 10002),  # valid
            (T0, T1, 2.0, -5.0, 10001, 10002),  # negative fare
            (T0, T1, 0.0, 10.0, 10001, 10002),  # zero distance
            (None, T1, 2.0, 10.0, 10001, 10002),  # null pickup
        ],
    )
    out = clean_trips(df).collect()
    assert len(out) == 1
    assert out[0]["trip_duration_min"] == 30.0
    assert len(out[0]["trip_id"]) == 64


def test_clean_trips_trip_id_is_deterministic(spark):
    df = raw(spark, [(T0, T1, 2.0, 10.0, 10001, 10002)])
    assert clean_trips(df).first()["trip_id"] == clean_trips(df).first()["trip_id"]


def test_dedupe_latest_keeps_most_recent_ingestion(spark):
    df = spark.createDataFrame(
        [("a", datetime(2024, 1, 1), "old"), ("a", datetime(2024, 1, 2), "new"), ("b", datetime(2024, 1, 1), "only")],
        ["trip_id", "_ingested_at", "v"],
    )
    out = {r["trip_id"]: r["v"] for r in dedupe_latest(df).collect()}
    assert out == {"a": "new", "b": "only"}


def test_trips_by_zip_hour_aggregates(spark):
    df = clean_trips(raw(spark, [(T0, T1, 2.0, 10.0, 10001, 1), (T0, T1, 3.0, 20.0, 10001, 2)]))
    row = trips_by_zip_hour(df).collect()
    assert len(row) == 1
    assert (row[0]["pickup_zip"], row[0]["pickup_hour"], row[0]["trips"], row[0]["avg_fare"]) == (10001, 8, 2, 15.0)
    assert row[0]["avg_duration_min"] == 30.0


def test_upsert_trips_is_idempotent_and_updates_matches(spark, tmp_path):
    path = str(tmp_path / "silver")
    batch = spark.createDataFrame([("a", 10.0), ("b", 20.0)], ["trip_id", "fare_amount"])
    batch.write.format("delta").save(path)
    target = DeltaTable.forPath(spark, path)

    upsert_trips(target, batch)  # same batch again
    assert target.toDF().count() == 2

    upsert_trips(target, spark.createDataFrame([("a", 99.0), ("c", 5.0)], ["trip_id", "fare_amount"]))
    out = {r["trip_id"]: r["fare_amount"] for r in target.toDF().collect()}
    assert out == {"a": 99.0, "b": 20.0, "c": 5.0}
