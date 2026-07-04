"""
gold_customer_features.py — Phase 6 Part A: per-customer LTV features.

This is a groupBy job (collapse many orders -> one row per customer). It answers
"who are my valuable / at-risk customers?" with:
    lifetime_revenue, lifetime_orders, avg_order_value,
    first_order_date, last_order_date, days_since_last_order (recency).

as_of_date is stamped so features are reproducible "as of" a point in time
(this matters for ML leakage -- Phase 12 goes deeper).
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F

AS_OF = "2026-07-04"   # pretend "today"; in prod this is a job parameter


def main():
    spark = get_spark("gold_customer_features")

    orders = (
        spark.read.parquet("data/silver/orders/")
        .filter(F.col("status") == "COMPLETE")     # LTV counts completed revenue
    )

    features = (
        orders
        .groupBy("tenant_id", "customer_id")
        .agg(
            F.sum("total_amount").alias("lifetime_revenue"),
            F.count("*").alias("lifetime_orders"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        # recency: days between last order and "today". datediff(end, start).
        .withColumn("days_since_last_order",
                    F.datediff(F.lit(AS_OF), F.col("last_order_date")))
        .withColumn("as_of_date", F.lit(AS_OF))
    )

    features.write.mode("overwrite").parquet("data/gold/customer_features/")

    print("=== sample: highest-value customers ===")
    (
        features.select("tenant_id", "customer_id", "lifetime_revenue",
                        "lifetime_orders", "avg_order_value", "days_since_last_order")
        .orderBy(F.desc("lifetime_revenue"))
        .show(10, truncate=False)
    )
    print(f"customers with features: {features.count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
