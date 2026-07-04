"""
salt_lab.py — Phase 9 Lab 5: fix the skewed join with SALTING, and measure it.

Problem (Lab 4): joining on tenant_id sends ALL of tenant_mega_001's rows to ONE
reduce partition -> one monster task.

Salting idea:
  - BIG side  (orders): add a random salt 0..N-1  -> new key (tenant_id, salt).
  - SMALL side (dim)  : REPLICATE every row across all N salts, so a salted order
                        still finds its matches.
  - Join on (tenant_id, salt). Now mega's rows are spread across N sub-keys ->
    N partitions -> N balanced tasks instead of one giant one.

Result is identical; only the physical distribution changes. We time both.
"""

import sys, os, time
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F

FANOUT = 30    # rows per tenant in the dim -> heavier per-task work so time skew shows
SALT = 16      # number of salt buckets to split the hot key into


def timed(label, build):
    t0 = time.perf_counter()
    n = build().count()
    print(f"  {label:<26} -> {n:,} rows in {(time.perf_counter()-t0)*1000:>7,.0f} ms")


def main():
    spark = get_spark("salt_lab")
    spark.conf.set("spark.sql.adaptive.enabled", "false")         # isolate the skew
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # force SortMergeJoin
    spark.conf.set("spark.sql.shuffle.partitions", "16")

    orders = spark.read.parquet("data/silver/orders/").select("tenant_id", "order_id")
    tenants = orders.select("tenant_id").distinct()
    dim = tenants.crossJoin(spark.range(FANOUT).withColumnRenamed("id", "campaign_id"))

    orders.count()  # warm cache

    # ---- A. skewed: join on tenant_id (all of mega -> 1 partition) ----
    print("\nA. SKEWED join on tenant_id:")
    timed("skewed", lambda: orders.join(dim, "tenant_id", "inner"))

    # ---- B. salted: split the hot key across SALT buckets ----
    print(f"\nB. SALTED join on (tenant_id, salt), SALT={SALT}:")
    salt_range = spark.range(SALT).select(F.col("id").cast("int").alias("salt"))
    dim_salted = dim.crossJoin(salt_range)                       # replicate dim across salts
    orders_salted = orders.withColumn("salt", (F.rand() * SALT).cast("int"))
    timed("salted",
          lambda: orders_salted.join(dim_salted, ["tenant_id", "salt"], "inner"))

    spark.stop()


if __name__ == "__main__":
    main()
