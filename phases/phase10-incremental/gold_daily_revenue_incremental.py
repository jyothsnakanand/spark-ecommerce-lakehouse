"""
gold_daily_revenue_incremental.py — Phase 10: process only what changed.

The full mart job overwrites ALL 261 date partitions every run. That doesn't
scale and can't cheaply "fix yesterday". This version:

  1. takes --start-date / --end-date  -> reads ONLY that date range (pruning),
  2. writes with partitionOverwriteMode=DYNAMIC -> overwrites ONLY the date
     partitions it actually produced, leaving every other date untouched.

Run examples:
  # one day
  python jobs/gold_daily_revenue_incremental.py --start-date 2026-06-01 --end-date 2026-06-01
  # a 7-day backfill
  python jobs/gold_daily_revenue_incremental.py --start-date 2026-06-01 --end-date 2026-06-07
"""

import sys, os, argparse
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

OUT = "data/gold/daily_revenue/"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", required=True)   # inclusive, 'YYYY-MM-DD'
    ap.add_argument("--end-date", required=True)     # inclusive
    args = ap.parse_args()

    spark = get_spark("gold_daily_revenue_incremental")
    # THE key setting: only replace partitions we write, keep the rest.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # read ONLY the window. Because silver/orders is partitioned by order_date,
    # this filter becomes a PartitionFilter -> we read just those folders.
    orders = (
        spark.read.parquet("data/silver/orders/")
        .filter((F.col("order_date") >= args.start_date) &
                (F.col("order_date") <= args.end_date) &
                (F.col("status") == "COMPLETE"))
        .select("tenant_id", "order_id", "order_date")
    )
    items = spark.read.parquet("data/silver/order_items/") \
        .select("tenant_id", "order_id", "product_id", "item_revenue")
    products = spark.read.parquet("data/silver/products/") \
        .select("tenant_id", "product_id", "category")

    mart = (
        items
        .join(orders, ["tenant_id", "order_id"], "inner")     # inner -> only in-window orders survive
        .join(broadcast(products), ["tenant_id", "product_id"], "left")
        .groupBy("tenant_id", "order_date", "category")
        .agg(F.sum("item_revenue").alias("revenue"),
             F.countDistinct("order_id").alias("orders"),
             F.count("*").alias("items_sold"))
    )

    n_parts = mart.select("order_date").distinct().count()
    (
        mart.write
        .mode("overwrite")                 # + dynamic mode = overwrite ONLY these dates
        .partitionBy("order_date")
        .parquet(OUT)
    )
    print(f"wrote {n_parts} date partition(s) for [{args.start_date} .. {args.end_date}]")
    spark.stop()


if __name__ == "__main__":
    main()
