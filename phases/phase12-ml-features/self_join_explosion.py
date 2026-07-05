"""
self_join_explosion.py — Phase 12: watch a self-join blow up quadratically.

A "frequently bought together" self-join emits, for an order with k items,
k*(k-1)/2 pairs. That's QUADRATIC in basket size, so a single huge basket can
dwarf millions of normal orders. This demo:
  1. tabulates the formula,
  2. confirms it empirically by self-joining synthetic baskets of sizes [3,10,50,200],
  3. PREDICTS the total pair count on real data from basket sizes alone (no join),
  4. shows what one 1000-item "wholesale" order would do,
  5. shows the fix: cap basket size before the self-join.
"""

import sys, os, time
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark
from pyspark.sql import functions as F


def pairs_via_selfjoin(df):
    """actually run the self-join and count pairs (per order)."""
    a, b = df.alias("a"), df.alias("b")
    return (a.join(b, (F.col("a.order_id") == F.col("b.order_id")) &
                       (F.col("a.pid") < F.col("b.pid")))
            .groupBy(F.col("a.order_id").alias("order_id")).count())


def main():
    spark = get_spark("self_join_explosion")

    # ---------- 1. the formula ----------
    print("1) pairs = k*(k-1)/2  (k = items in the basket):")
    for k in [2, 4, 10, 50, 100, 500, 1000]:
        print(f"   k={k:>4}  ->  {k*(k-1)//2:>9,} pairs")

    # ---------- 2. empirical confirmation ----------
    print("\n2) build synthetic baskets and RUN the self-join:")
    rows = []
    for oid, k in [("o_k3", 3), ("o_k10", 10), ("o_k50", 50), ("o_k200", 200)]:
        rows += [(oid, f"p{i:04d}") for i in range(k)]      # k items in this order
    synth = spark.createDataFrame(rows, ["order_id", "pid"])
    counted = pairs_via_selfjoin(synth).orderBy("count")
    print("   order -> actual pairs from the self-join (compare to k*(k-1)/2):")
    counted.show(truncate=False)

    # ---------- 3. predict real-data pairs WITHOUT joining ----------
    print("3) real order_items: predict total pairs from basket sizes (no self-join needed):")
    baskets = (spark.read.parquet("data/silver/order_items/")
               .groupBy("tenant_id", "order_id").agg(F.count("*").alias("k")))
    stats = baskets.agg(F.min("k").alias("min_k"), F.round(F.avg("k"), 2).alias("avg_k"),
                        F.max("k").alias("max_k"), F.count("*").alias("orders")).first()
    predicted = baskets.select(F.sum(F.col("k") * (F.col("k") - 1) / 2).alias("pairs")).first()["pairs"]
    print(f"   baskets: {stats['orders']:,} orders, size min={stats['min_k']} "
          f"avg={stats['avg_k']} max={stats['max_k']}")
    print(f"   predicted total pairs (sum of k*(k-1)/2): {int(predicted):,}")

    # ---------- 4. one wholesale order ----------
    big_k = 1000
    print(f"\n4) add ONE {big_k}-item wholesale order:")
    print(f"   that single order alone adds {big_k*(big_k-1)//2:,} pairs "
          f"-> ~{(big_k*(big_k-1)//2)/predicted*100:.0f}% on top of ALL {stats['orders']:,} real orders.")

    # ---------- 5. the fix: cap basket size ----------
    CAP = 20
    capped = baskets.filter(F.col("k") <= CAP)
    capped_pairs = capped.select(F.sum(F.col("k") * (F.col("k") - 1) / 2).alias("p")).first()["p"]
    dropped = stats["orders"] - capped.count()
    print(f"\n5) FIX: drop baskets with > {CAP} items before the self-join.")
    print(f"   removes {dropped:,} outlier orders, pairs -> {int(capped_pairs):,} "
          "(bounds the blast radius).")

    spark.stop()


if __name__ == "__main__":
    main()
