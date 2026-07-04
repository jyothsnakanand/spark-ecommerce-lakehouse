"""
streaming_click_metrics.py — Phase 11: a file-source streaming aggregation.

Watches data/landing/clickstream_stream/ for new JSON files and continuously
computes events-per-1-minute-window per event_type.

Key streaming pieces:
  readStream        - the streaming twin of read; input is UNBOUNDED.
  explicit schema   - REQUIRED for file streams (can't infer from a moving target).
  withWatermark     - "expect data up to 2 min late; after that, finalize windows".
  window(...)       - event-time bucketing (uses event_ts, not clock time).
  checkpointLocation- remembers which files/offsets are done -> fault tolerance
                      + incremental reruns (already-seen files are skipped).
  trigger(availableNow) - process all currently-available files in micro-batches,
                      then STOP. (Reproducible demo instead of an infinite stream.)
  maxFilesPerTrigger=1   - one file per micro-batch, so we SEE batches accumulate.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

STREAM_DIR = "data/landing/clickstream_stream"
CHECKPOINT = "data/checkpoints/click_metrics"

# explicit schema — file streams cannot infer
CLICK_SCHEMA = StructType([
    StructField("tenant_id",   StringType(),    True),
    StructField("event_id",    StringType(),    True),
    StructField("session_id",  StringType(),    True),
    StructField("customer_id", StringType(),    True),
    StructField("event_ts",    TimestampType(), True),
    StructField("event_date",  StringType(),    True),
    StructField("event_type",  StringType(),    True),
    StructField("product_id",  StringType(),    True),
    StructField("page_url",    StringType(),    True),
    StructField("device_type", StringType(),    True),
])


def main():
    spark = get_spark("streaming_click_metrics")

    events = (
        spark.readStream
        .schema(CLICK_SCHEMA)
        .option("maxFilesPerTrigger", 1)          # 1 file = 1 micro-batch (for visibility)
        .json(STREAM_DIR)
    )

    per_minute = (
        events
        .withWatermark("event_ts", "2 minutes")   # tolerate 2 min of lateness
        .groupBy(
            F.window("event_ts", "1 minute").alias("minute"),
            "event_type",
        )
        .count()
    )

    # flatten the window struct into readable start/end columns for the console
    out = per_minute.select(
        F.col("minute.start").alias("win_start"),
        F.col("minute.end").alias("win_end"),
        "event_type", "count",
    )

    query = (
        out.writeStream
        .format("console")
        .outputMode("update")                     # show windows that changed this batch
        .option("truncate", "false")
        .option("numRows", 50)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)               # drain existing files, then stop
        .start()
    )
    query.awaitTermination()
    print("\nstream finished (availableNow drained all files).")
    spark.stop()


if __name__ == "__main__":
    main()
