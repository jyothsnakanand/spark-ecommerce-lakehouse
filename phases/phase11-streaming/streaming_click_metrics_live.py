"""
streaming_click_metrics_live.py — Phase 11 (LIVE): a continuously-running stream.

Unlike the availableNow version, this one runs FOREVER (awaitTermination) with a
clock-based trigger, so you can drip new files in and watch it react in ~real time.

  input:      data/landing/clickstream_live/   (feed with generate_clickstream --now --out ...)
  checkpoint: data/checkpoints/click_live/
  trigger:    every 4 seconds (a micro-batch fires on the clock)

Run unbuffered so console output streams out live:
    python -u phases/phase11-streaming/streaming_click_metrics_live.py
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

LIVE_DIR = "data/landing/clickstream_live"
CHECKPOINT = "data/checkpoints/click_live"

CLICK_SCHEMA = StructType([
    StructField("tenant_id", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("event_date", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("device_type", StringType(), True),
])


def main():
    spark = get_spark("streaming_click_metrics_live")
    os.makedirs(LIVE_DIR, exist_ok=True)

    events = (spark.readStream.schema(CLICK_SCHEMA)
              .option("maxFilesPerTrigger", 5)
              .json(LIVE_DIR))

    per_min = (events
               .withWatermark("event_ts", "1 minute")
               .groupBy(F.window("event_ts", "1 minute").alias("m"), "event_type")
               .count())

    out = per_min.select(F.date_format("m.start", "HH:mm").alias("minute"),
                         "event_type", "count")

    query = (out.writeStream.format("console")
             .outputMode("update").option("truncate", "false").option("numRows", 40)
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime="4 seconds")     # a batch every 4s
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
