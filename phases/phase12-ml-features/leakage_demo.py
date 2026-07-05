"""
leakage_demo.py — Phase 12: SHOW data leakage, don't just warn about it.

Setup: pick AS_OF in the middle of the data.
  - LABEL   = did the customer buy in the 30 days AFTER as_of? (the thing a model
              would try to predict — it legitimately uses the future.)
  - FEATURE = "recency": days since the customer's last order.
              computed two ways:
                leaky   -> max(order_date) over ALL orders (peeks into the future)
                correct -> max(order_date) using only orders <= as_of (point-in-time)

The leaky feature secretly contains future purchase info, so:
  1. it produces IMPOSSIBLE values (negative recency = "last order hasn't happened yet"),
  2. it separates the label almost perfectly in dev... then is useless in production,
     because at scoring time you cannot know the future.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark
from pyspark.sql import functions as F

AS_OF = "2026-05-20"
FUTURE_END = "2026-06-19"   # AS_OF + 30 days


def main():
    spark = get_spark("leakage_demo")

    orders = (spark.read.parquet("data/silver/orders/")
              .filter(F.col("status") == "COMPLETE")
              .select("tenant_id", "customer_id", "order_date"))

    per_cust = orders.groupBy("tenant_id", "customer_id").agg(
        # LEAKY: last order over ALL data (includes the future)
        F.max("order_date").alias("leaky_last_order"),
        # CORRECT: last order on/before as_of (point-in-time)
        F.max(F.when(F.col("order_date") <= AS_OF, F.col("order_date"))).alias("pit_last_order"),
        # LABEL: any order in (as_of, as_of+30d]?
        F.max(F.when((F.col("order_date") > AS_OF) & (F.col("order_date") <= FUTURE_END), 1)
              .otherwise(0)).alias("bought_next_30d"),
    ).withColumn("leaky_recency",   F.datediff(F.lit(AS_OF), F.col("leaky_last_order"))) \
     .withColumn("correct_recency", F.datediff(F.lit(AS_OF), F.col("pit_last_order")))

    # only customers who existed by as_of (had a past order) are scorable
    scorable = per_cust.filter(F.col("pit_last_order").isNotNull())

    total = scorable.count()
    negative_leaky = scorable.filter(F.col("leaky_recency") < 0).count()
    print(f"AS_OF = {AS_OF}   scorable customers = {total:,}")
    print(f"\n1) IMPOSSIBLE VALUES: customers whose LEAKY recency is NEGATIVE "
          f"(last order in the future): {negative_leaky:,}  ({100*negative_leaky/total:.1f}%)")
    print("   Negative 'days since last order' is impossible -> a red flag for leakage.\n")

    print("2) SAME CUSTOMERS, two features side by side (note leaky < 0 = future):")
    (scorable.filter(F.col("leaky_recency") < 0)
     .select("customer_id", "correct_recency", "leaky_recency",
             "pit_last_order", "leaky_last_order", "bought_next_30d")
     .show(6, truncate=False))

    print("3) MEAN recency by label — a leaky feature separates the label suspiciously well:")
    (scorable.groupBy("bought_next_30d").agg(
        F.round(F.avg("correct_recency"), 1).alias("avg_correct_recency"),
        F.round(F.avg("leaky_recency"), 1).alias("avg_LEAKY_recency"),
        F.count("*").alias("customers"),
    ).orderBy("bought_next_30d").show(truncate=False))
    print("   correct: buyers have only slightly lower recency (mild real signal).")
    print("   LEAKY:   buyers have NEGATIVE avg recency -> the feature basically IS the label.")

    spark.stop()


if __name__ == "__main__":
    main()
