"""
apply_cdc.py — Phase 15: turn a change log into current state (the CDC merge).

  bronze = the raw, append-only CHANGE LOG (every I/U/D, immutable, replayable).
  silver = the MATERIALIZED CURRENT STATE: for each key, the latest change wins,
           and DELETE tombstones remove the row.

This "latest-per-key, drop deletes" merge is the backbone of every lakehouse
ingestion from an OLTP source. Delta/Iceberg `MERGE INTO` automates exactly this.
It's idempotent: replaying the whole log always reconstructs the same state.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType,
                               LongType, DoubleType)

CDC_DIR = "data/landing/orders_cdc"
SILVER = "data/silver/orders_current"

SCHEMA = StructType([
    StructField("op", StringType(), True),            # I / U / D
    StructField("commit_ts", TimestampType(), True),
    StructField("seq", LongType(), True),
    StructField("tenant_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
])


def main():
    spark = get_spark("apply_cdc")

    # bronze: the raw change log (as it arrived, order not guaranteed)
    changelog = spark.read.schema(SCHEMA).json(CDC_DIR)
    total = changelog.count()
    print("change events by op:")
    changelog.groupBy("op").count().orderBy("op").show()

    # MERGE: latest event per (tenant_id, order_id). Tiebreak commit_ts with seq,
    # so out-of-order arrival never changes the winner.
    w = Window.partitionBy("tenant_id", "order_id").orderBy(
        F.col("commit_ts").desc(), F.col("seq").desc())
    latest = changelog.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)

    tombstones = latest.filter(F.col("op") == "D").count()
    current = latest.filter(F.col("op") != "D").drop("rn", "op", "seq")   # apply deletes

    current.write.mode("overwrite").parquet(SILVER)

    distinct_orders = latest.count()
    current_rows = current.count()
    print(f"total change events   : {total:,}")
    print(f"distinct orders       : {distinct_orders:,}")
    print(f"tombstoned (deleted)  : {tombstones:,}")
    print(f"current-state rows     : {current_rows:,}  "
          f"(= distinct {distinct_orders:,} - deleted {tombstones:,})")

    print("\ncurrent status distribution (deletes are GONE, PENDING/COMPLETE remain):")
    current.groupBy("status").count().orderBy("status").show()

    spark.stop()


if __name__ == "__main__":
    main()
