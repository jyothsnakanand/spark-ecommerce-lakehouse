"""
salt_visual.py — a 6-row, printable walkthrough of salting.
Tiny data + SALT=3 so you can eyeball every step.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark
from pyspark.sql import functions as F

SALT = 3

spark = get_spark("salt_visual")

# 4 orders for the hot tenant 'mega', 2 for a normal tenant 'small'
orders = spark.createDataFrame(
    [("mega", "o1"), ("mega", "o2"), ("mega", "o3"), ("mega", "o4"),
     ("small", "s1"), ("small", "s2")],
    ["tenant_id", "order_id"],
)
# dim: 2 campaigns per tenant (FANOUT=2)
dim = spark.createDataFrame(
    [("mega", "c0"), ("mega", "c1"), ("small", "c0"), ("small", "c1")],
    ["tenant_id", "campaign_id"],
)

print("\n1) ORDERS — unsalted key is just tenant_id (all 4 'mega' share one key):")
orders.show()

print("2) BIG side salted — each order rolls ONE salt (0..2):")
orders_salted = orders.withColumn("salt", (F.rand(seed=7) * SALT).cast("int"))
orders_salted.orderBy("tenant_id", "order_id").show()

print("3) SMALL side replicated across ALL salts (crossJoin) — 4 rows x 3 salts = 12:")
salt_range = spark.range(SALT).select(F.col("id").cast("int").alias("salt"))
dim_salted = dim.crossJoin(salt_range)
dim_salted.orderBy("tenant_id", "salt", "campaign_id").show(20)

print("4) JOIN on (tenant_id, salt) — each order still matches exactly its 2 campaigns:")
joined = orders_salted.join(dim_salted, ["tenant_id", "salt"], "inner")
joined.orderBy("order_id", "campaign_id").show(30)

print("5) DISTRIBUTION — the single 'mega' key is now spread across salt buckets:")
(orders_salted.groupBy("tenant_id", "salt").count()
 .orderBy("tenant_id", "salt").show())

print(f"unsalted distinct join keys: {orders.select('tenant_id').distinct().count()}  "
      f"(mega is 1 hot key)")
print(f"salted   distinct join keys: {orders_salted.select('tenant_id','salt').distinct().count()}  "
      f"(mega spread across buckets)")

spark.stop()
