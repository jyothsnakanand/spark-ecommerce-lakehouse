"""
spill_demo.py — Phase 9: make the Spill columns light up, on purpose.

Two things force spill here:
  1. spark.sql.shuffle.partitions = 2  -> only 2 huge tasks sort ~1.2M rows each.
  2. spark.shuffle.spill.numElementsForceSpillThreshold = 50000
     -> the sorter is TOLD to spill to disk after 50k elements, guaranteeing it.
AQE is off (so it can't coalesce/rescue) and broadcast is off (so it's a real
SortMergeJoin that sorts both sides).

The job runs, then the process SLEEPS so the Spark UI stays alive. Go to:
    http://localhost:4040  ->  Stages  ->  the SortMergeJoin stage
Look at the 'Spill (Memory)' and 'Spill (Disk)' columns, and in that stage's
Summary Metrics table, the 'Spill (Memory)' / 'Spill (Disk)' rows.
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("spill_demo")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")            # no AQE rescue
    .config("spark.sql.shuffle.partitions", "2")              # 2 huge tasks
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")     # real SortMergeJoin
    .config("spark.shuffle.spill.numElementsForceSpillThreshold", "50000")  # force spill
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setJobDescription("SPILL DEMO: SMJ items x orders, 2 partitions")

orders = spark.read.parquet("data/silver/orders/").select("tenant_id", "order_id")
items = spark.read.parquet("data/silver/order_items/") \
    .select("tenant_id", "order_id", "item_revenue")

joined = items.join(orders, ["tenant_id", "order_id"], "inner")

t0 = time.perf_counter()
n = joined.count()
print(f"\njoined {n:,} rows in {(time.perf_counter()-t0)*1000:,.0f} ms")
print("Spark UI is live at http://localhost:4040  (Stages -> SortMergeJoin stage -> Spill columns)")
print("This process will stay up for 10 minutes so you can inspect it.")

time.sleep(600)   # keep the UI alive
spark.stop()
