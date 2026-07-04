"""
perf_lab_shuffle.py — Phase 9 Labs 1&2: the spark.sql.shuffle.partitions knob.

We run the SAME heavy shuffle (a forced SortMergeJoin of items x orders, which
moves ALL ~3.5M rows) under different partition counts and time it:

  too FEW  (4)   -> a handful of huge tasks -> spill to disk, poor parallelism
  balanced (~cores..200)
  too MANY (1000)-> hundreds of near-empty tasks -> scheduling overhead

Then we turn AQE back ON to show it auto-coalesces a silly 1000 down to a
sensible number at runtime.

While this runs, open http://localhost:4040 -> Stages, and watch:
  - the Tasks count per stage change with the setting
  - the 'Spill (Memory/Disk)' columns light up at 4 partitions
"""

import sys, os, time
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F


def run(spark, n_parts, aqe):
    spark.conf.set("spark.sql.adaptive.enabled", "true" if aqe else "false")
    spark.conf.set("spark.sql.shuffle.partitions", str(n_parts))
    # force a SortMergeJoin (no broadcast) so the WHOLE thing shuffles
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    orders = spark.read.parquet("data/silver/orders/").select("tenant_id", "order_id")
    items = spark.read.parquet("data/silver/order_items/") \
        .select("tenant_id", "order_id", "item_revenue")

    joined = items.join(orders, ["tenant_id", "order_id"], "inner")

    t0 = time.perf_counter()
    n = joined.count()
    dt = (time.perf_counter() - t0) * 1000
    label = f"AQE={'on ' if aqe else 'off'}  shuffle.partitions={n_parts:>5}"
    print(f"  {label}  ->  {n:,} rows joined in {dt:>7,.0f} ms")


def main():
    spark = get_spark("perf_lab_shuffle")

    # warm up (page the parquet into OS cache so timings compare fairly)
    spark.read.parquet("data/silver/order_items/").count()
    print("\n--- AQE OFF: raw effect of the partition count ---")
    for n in [4, 16, 200, 1000]:
        run(spark, n, aqe=False)

    print("\n--- AQE ON: it coalesces a silly 1000 at runtime ---")
    run(spark, 1000, aqe=True)

    print("\n(leave this process a moment and refresh the Spark UI Stages tab)")
    spark.stop()


if __name__ == "__main__":
    main()
