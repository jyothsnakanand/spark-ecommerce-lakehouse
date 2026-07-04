"""
skew_lab.py — Phase 9 Lab 4: manufacture a skewed join and SEE the long tail.

We join orders (skewed: tenant_mega_001 owns ~60%) to a small per-tenant
dimension ON tenant_id. Because the JOIN KEY is the skewed column, when Spark
shuffles by tenant_id, ALL of mega's rows hash to ONE reduce partition -> one
monster task while the others finish early. That's the long tail.

FANOUT gives the dimension several rows per tenant, so the hot task does real
work (mega: ~600k * FANOUT rows on a single task).

Config: SortMergeJoin forced (no broadcast), AQE OFF (so it can't auto-fix the
skew) -> the raw, unmitigated problem.

Runs the join, prints timing, then SLEEPS so you can inspect the Spark UI:
    http://localhost:4040 (or 4041)  ->  Stages  ->  the SortMergeJoin stage
    -> Summary Metrics: compare Median vs Max for Duration and Shuffle Read.
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FANOUT = 10

spark = (
    SparkSession.builder.appName("skew_lab").master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")          # don't let AQE fix it
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")   # force SortMergeJoin
    .config("spark.sql.shuffle.partitions", "16")           # tenants spread, mega isolated
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setJobDescription("SKEW: orders ⋈ tenant_dim on tenant_id")

orders = spark.read.parquet("data/silver/orders/").select("tenant_id", "order_id", "total_amount")

# show the skew we're about to weaponize
print("\norders per tenant (the skew):")
orders.groupBy("tenant_id").count().orderBy(F.desc("count")).show(truncate=False)

# build a small per-tenant dimension with FANOUT rows per tenant
tenants = orders.select("tenant_id").distinct()
dim = tenants.crossJoin(spark.range(FANOUT).withColumnRenamed("id", "campaign_id"))
print(f"dimension rows (10 tenants x {FANOUT}): {dim.count()}")

# the skewed join: key = tenant_id (the hot column)
joined = orders.join(dim, "tenant_id", "inner")

t0 = time.perf_counter()
n = joined.count()
print(f"\nskewed join produced {n:,} rows in {(time.perf_counter()-t0)*1000:,.0f} ms")
print("Inspect the SortMergeJoin stage in the Spark UI: Median vs Max task will be lopsided.")
print("(process sleeps 10 min so the UI stays alive)")
time.sleep(600)
spark.stop()
