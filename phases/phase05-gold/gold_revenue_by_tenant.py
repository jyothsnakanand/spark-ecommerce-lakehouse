"""
gold_revenue_by_tenant.py — the "first real job" from the lesson plan.

Reads bronze orders, keeps only COMPLETE orders, and computes total revenue
per tenant. The GOAL is not the number -- it's to SEE, in the physical plan,
the boundary between a narrow transformation (filter) and a wide one (groupBy),
and to identify exactly where Spark shuffles.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql.functions import col, sum as _sum   # rename: sum shadows builtin


def main():
    spark = get_spark("gold_revenue_by_tenant")

    orders = spark.read.parquet("data/silver/orders/")   # gold builds on SILVER, not bronze

    revenue_by_tenant = (
        orders
        .filter(col("status") == "COMPLETE")             # NARROW: per-row, no data movement
        .groupBy("tenant_id")                            # WIDE:   needs all rows per tenant together
        .agg(_sum("total_amount").alias("revenue"))      # -> triggers a shuffle
    )

    print("=== PHYSICAL PLAN ===")
    revenue_by_tenant.explain("formatted")               # inspect BEFORE the action runs

    print("\n=== RESULT (watch the skew!) ===")
    revenue_by_tenant.orderBy(col("revenue").desc()).show(20, truncate=False)

    (
        revenue_by_tenant.write
        .mode("overwrite")
        .parquet("data/gold/revenue_by_tenant/")
    )

    spark.stop()


if __name__ == "__main__":
    main()
