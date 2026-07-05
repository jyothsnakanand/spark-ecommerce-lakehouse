"""
metrics_harness.py — Phase 14: emit job metrics on every run.

Wraps the daily_revenue mart build and records, to an append-only JSONL log:
  runtime · input rows · output rows · rejected rows · output files/bytes ·
  shuffle read/write bytes (pulled from Spark's REST API).

Why: a job that silently drops half its rows throws NO error. Emitting metrics
every run makes regressions visible (row-count drop, file-count explosion,
runtime spike) BEFORE a human notices a broken dashboard. A downstream test
(test_job_metrics.py) then asserts these stay within expected bands.
"""

import sys, os, time, json, datetime
from urllib.request import urlopen
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

OUT = "data/gold/daily_revenue/"
METRICS_LOG = "data/gold/_job_metrics/metrics.jsonl"


def dir_stats(path):
    """count parquet files and total bytes under a path."""
    files = total = 0
    for root, _, fs in os.walk(path):
        for f in fs:
            if f.endswith(".parquet"):
                files += 1
                total += os.path.getsize(os.path.join(root, f))
    return files, total


def shuffle_bytes(spark):
    """pull total shuffle read/write bytes from the Spark REST API (UI must be live)."""
    try:
        base = spark.sparkContext.uiWebUrl                 # e.g. http://host:4040
        app = spark.sparkContext.applicationId
        stages = json.load(urlopen(f"{base}/api/v1/applications/{app}/stages", timeout=5))
        rd = sum(s.get("shuffleReadBytes", 0) for s in stages)
        wr = sum(s.get("shuffleWriteBytes", 0) for s in stages)
        return rd, wr
    except Exception as e:
        print(f"  (shuffle metrics unavailable: {type(e).__name__})")
        return None, None


def main():
    spark = get_spark("gold_daily_revenue")
    spark.sparkContext.setJobDescription("gold_daily_revenue")
    t0 = time.perf_counter()

    orders = (spark.read.parquet("data/silver/orders/")
              .filter(F.col("status") == "COMPLETE")
              .select("tenant_id", "order_id", "order_date"))
    items = (spark.read.parquet("data/silver/order_items/")
             .select("tenant_id", "order_id", "product_id", "item_revenue"))
    products = (spark.read.parquet("data/silver/products/")
                .select("tenant_id", "product_id", "category"))

    input_orders, input_items = orders.count(), items.count()   # input-row metrics

    mart = (items.join(orders, ["tenant_id", "order_id"], "inner")
            .join(broadcast(products), ["tenant_id", "product_id"], "left")
            .groupBy("tenant_id", "order_date", "category")
            .agg(F.sum("item_revenue").alias("revenue"),
                 F.countDistinct("order_id").alias("orders"),
                 F.count("*").alias("items_sold")))
    mart.write.mode("overwrite").partitionBy("order_date").parquet(OUT)

    output_rows = spark.read.parquet(OUT).count()
    rejected = spark.read.parquet("data/silver/rejected_orders/").count()
    runtime = time.perf_counter() - t0
    files, obytes = dir_stats(OUT)
    sh_rd, sh_wr = shuffle_bytes(spark)     # while the UI is still alive

    rec = {
        "ts": datetime.datetime.now().isoformat(timespec="seconds"),
        "job": "gold_daily_revenue",
        "runtime_s": round(runtime, 1),
        "input_orders": input_orders,
        "input_items": input_items,
        "output_rows": output_rows,
        "rejected_orders": rejected,
        "output_files": files,
        "output_mb": round(obytes / 1e6, 2),
        "shuffle_read_mb": None if sh_rd is None else round(sh_rd / 1e6, 2),
        "shuffle_write_mb": None if sh_wr is None else round(sh_wr / 1e6, 2),
    }
    os.makedirs(os.path.dirname(METRICS_LOG), exist_ok=True)
    with open(METRICS_LOG, "a") as f:
        f.write(json.dumps(rec) + "\n")

    print("\n=== JOB METRICS (appended to metrics.jsonl) ===")
    for k, v in rec.items():
        print(f"  {k:<18}: {v}")

    spark.stop()


if __name__ == "__main__":
    main()
