"""
partition_lab.py — Phase 7: why physical file layout matters.

We write the SAME orders data three ways and MEASURE the difference:
  A. GOOD  : partitionBy(order_date)            -> query one day reads one folder
  B. BAD   : partitionBy(order_id)  (sampled!)  -> thousands of tiny folders/files
  C. TUNED : repartition(order_date) then write -> one tidy file per partition

Metrics per layout: #directories, #parquet files, total size, and the time +
partition-pruning behaviour of a "one day" query.
"""

import sys, os, time
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F

LAB = "data/lab"


def fs_report(path):
    """Count partition dirs, parquet files, and total bytes under a path."""
    n_dirs = n_files = total = 0
    for root, dirs, files in os.walk(path):
        for d in dirs:
            if "=" in d:                      # a partition folder like order_date=...
                n_dirs += 1
        for f in files:
            if f.endswith(".parquet"):
                n_files += 1
                total += os.path.getsize(os.path.join(root, f))
    print(f"    partition dirs : {n_dirs:,}")
    print(f"    parquet files  : {n_files:,}")
    print(f"    total size     : {total/1_000_000:.2f} MB")
    if n_files:
        print(f"    avg file size  : {total/n_files/1024:.1f} KB")


def timed(label, fn):
    t0 = time.perf_counter()
    result = fn()
    dt = (time.perf_counter() - t0) * 1000
    print(f"    {label}: {result:,} rows in {dt:,.0f} ms")


def main():
    spark = get_spark("partition_lab")
    orders = spark.read.parquet("data/silver/orders/")

    # a date that actually has lots of orders, so the query is meaningful
    busy_date = (orders.groupBy("order_date").count()
                 .orderBy(F.desc("count")).first()["order_date"])
    print(f"busy_date chosen for the query test: {busy_date}\n")

    # ================= A. GOOD: partition by date =================
    print("A. GOOD  partitionBy(order_date)")
    orders.write.mode("overwrite").partitionBy("order_date") \
        .parquet(f"{LAB}/orders_by_date/")
    fs_report(f"{LAB}/orders_by_date/")
    by_date = spark.read.parquet(f"{LAB}/orders_by_date/")
    timed("query order_date==busy_date",
          lambda: by_date.filter(F.col("order_date") == busy_date).count())
    print("    plan (look for PartitionFilters -> pruning):")
    by_date.filter(F.col("order_date") == busy_date) \
        .select("order_id").explain()

    # ================= B. BAD: partition by order_id (sampled) =================
    print("\nB. BAD   partitionBy(order_id)  [3,000-row sample to spare your FS]")
    sample = orders.limit(3000)
    sample.write.mode("overwrite").partitionBy("order_id") \
        .parquet(f"{LAB}/orders_by_order_id/")
    fs_report(f"{LAB}/orders_by_order_id/")
    by_id = spark.read.parquet(f"{LAB}/orders_by_order_id/")
    timed("query order_date==busy_date (NO pruning possible)",
          lambda: by_id.filter(F.col("order_date") == busy_date).count())

    # ================= C. TUNED: one file per date partition =================
    print("\nC. TUNED repartition(order_date) then partitionBy(order_date)")
    orders.repartition("order_date").write.mode("overwrite") \
        .partitionBy("order_date").parquet(f"{LAB}/orders_tuned/")
    fs_report(f"{LAB}/orders_tuned/")

    spark.stop()


if __name__ == "__main__":
    main()
