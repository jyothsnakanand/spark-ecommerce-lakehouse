"""
product_affinity.py — Phase 12: "frequently bought together" via a SELF-JOIN.

To find product PAIRS that appear in the same order, we join order_items to
ITSELF on (tenant_id, order_id). The trick a.product_id < b.product_id:
  - drops self-pairs (A,A),
  - keeps each unordered pair once ((A,B) not also (B,A)).

WHY SELF-JOINS ARE DANGEROUS: an order with k items yields k*(k-1)/2 pairs.
k=4 -> 6 pairs (fine). k=100 -> 4,950 pairs. k=1000 -> ~500k pairs from ONE order.
Self-joins can explode data quadratically -- always cap basket size or pre-filter.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark
from pyspark.sql import functions as F

TOP = 15


def main():
    spark = get_spark("product_affinity")

    items = (spark.read.parquet("data/silver/order_items/")
             .select("tenant_id", "order_id", "product_id").distinct())  # one product per order

    a = items.alias("a")
    b = items.alias("b")

    pairs = (
        a.join(b, (F.col("a.tenant_id") == F.col("b.tenant_id")) &
                   (F.col("a.order_id") == F.col("b.order_id")) &
                   (F.col("a.product_id") < F.col("b.product_id")))   # unordered, no self-pair
        .select(F.col("a.tenant_id").alias("tenant_id"),
                F.col("a.product_id").alias("product_a"),
                F.col("b.product_id").alias("product_b"))
    )

    affinity = (pairs.groupBy("tenant_id", "product_a", "product_b")
                .agg(F.count("*").alias("co_occurrences"))
                .filter(F.col("co_occurrences") >= 2))

    affinity.write.mode("overwrite").parquet("data/gold/product_affinity/")

    print(f"distinct co-purchased pairs: {affinity.count():,}")
    print(f"top {TOP} most-frequently-bought-together (tenant_mega_001):")
    (affinity.filter(F.col("tenant_id") == "tenant_mega_001")
     .orderBy(F.desc("co_occurrences"))
     .show(TOP, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
