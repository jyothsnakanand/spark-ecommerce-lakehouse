"""
gold_customer_features_ml.py — Phase 12: leakage-safe, time-windowed features.

Upgrades over the Phase 6 version:
  1. POINT-IN-TIME: everything is computed "as of" AS_OF using only orders with
     order_date <= AS_OF. You must never use the future to build a feature that
     predicts the past -- that's LEAKAGE (great dev metrics, dead in production).
  2. TIME WINDOWS: 7d / 30d activity, not just lifetime -- recency-weighted signal.
  3. COMPLETENESS: LEFT JOIN back to the full customer dimension so customers with
     zero orders still get a row (with 0s), instead of silently vanishing.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark
from pyspark.sql import functions as F

AS_OF = "2026-07-04"


def main():
    spark = get_spark("gold_customer_features_ml")

    customers = spark.read.parquet("data/silver/customers/").select("tenant_id", "customer_id")

    # POINT-IN-TIME cut: only orders on/before AS_OF (no peeking into the future)
    orders = (
        spark.read.parquet("data/silver/orders/")
        .filter((F.col("status") == "COMPLETE") & (F.col("order_date") <= AS_OF))
        .withColumn("days_ago", F.datediff(F.lit(AS_OF), F.col("order_date")))
    )

    # conditional aggregation: sum/count only rows inside each trailing window
    agg = (
        orders.groupBy("tenant_id", "customer_id").agg(
            F.sum(F.when(F.col("days_ago") <= 7, F.col("total_amount")).otherwise(0)).alias("revenue_7d"),
            F.sum(F.when(F.col("days_ago") <= 7, 1).otherwise(0)).alias("orders_7d"),
            F.sum(F.when(F.col("days_ago") <= 30, F.col("total_amount")).otherwise(0)).alias("revenue_30d"),
            F.sum(F.when(F.col("days_ago") <= 30, 1).otherwise(0)).alias("orders_30d"),
            F.sum("total_amount").alias("lifetime_revenue"),
            F.count("*").alias("lifetime_orders"),
            F.min("days_ago").alias("days_since_last_order"),
        )
    )

    # COMPLETENESS: keep every customer; fill non-buyers with 0 / null recency
    features = (
        customers.join(agg, ["tenant_id", "customer_id"], "left")
        .na.fill(0, ["revenue_7d", "orders_7d", "revenue_30d", "orders_30d",
                     "lifetime_revenue", "lifetime_orders"])
        .withColumn("as_of_date", F.lit(AS_OF))
    )

    features.write.mode("overwrite").partitionBy("as_of_date").parquet("data/gold/customer_features_ml/")

    print(f"feature rows: {features.count():,}  (should equal customer count)")
    print("sample (recent high-value customers):")
    (features.orderBy(F.desc("revenue_30d"))
     .select("tenant_id", "customer_id", "orders_7d", "revenue_7d",
             "orders_30d", "revenue_30d", "lifetime_revenue", "days_since_last_order")
     .show(8, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
