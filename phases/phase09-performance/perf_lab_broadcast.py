"""
perf_lab_broadcast.py — Phase 9 Lab 3: broadcast vs shuffle join, measured.

Same join (2.5M items x 2k products), two ways:
  A. SHUFFLE (SortMergeJoin): autoBroadcast disabled -> BOTH sides shuffled+sorted.
  B. BROADCAST (BroadcastHashJoin): ship the 2k-row products to every task ->
     items is NOT shuffled at all.

AQE stays OFF so our config choice is what actually runs (AQE would broadcast
the tiny table on its own and hide the contrast).
"""

import sys, os, time
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def timed(spark, label, build_join):
    t0 = time.perf_counter()
    n = build_join().count()
    dt = (time.perf_counter() - t0) * 1000
    print(f"  {label:<34} -> {n:,} rows in {dt:>7,.0f} ms")


def main():
    spark = get_spark("perf_lab_broadcast")
    spark.conf.set("spark.sql.adaptive.enabled", "false")   # so OUR choice runs

    items = spark.read.parquet("data/silver/order_items/") \
        .select("tenant_id", "product_id", "item_revenue")
    products = spark.read.parquet("data/silver/products/") \
        .select("tenant_id", "product_id", "category")

    items.count()  # warm cache

    print("\nA. SHUFFLE join (SortMergeJoin, broadcast disabled):")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    timed(spark, "  items ⋈ products (SMJ)",
          lambda: items.join(products, ["tenant_id", "product_id"], "inner"))

    print("\nB. BROADCAST join (BroadcastHashJoin):")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    timed(spark, "  items ⋈ broadcast(products)",
          lambda: items.join(broadcast(products), ["tenant_id", "product_id"], "inner"))

    spark.stop()


if __name__ == "__main__":
    main()
