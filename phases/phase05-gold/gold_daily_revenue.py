"""
gold_daily_revenue.py — Phase 5: the business-facing revenue mart.

Question answered: "revenue by tenant, day, and category?"

Grain of revenue = order_items (that's where item_revenue lives). So items is
the SPINE; we join orders (for the date + completed-status) and products (for
the category) onto it, then aggregate.

Key ideas demonstrated:
  - FILTER EARLY: keep only COMPLETE orders BEFORE the join -> smaller shuffle.
  - PRUNE EARLY: select only the columns each side needs -> less data moved.
  - BROADCAST the small products dimension -> no shuffle of it.
  - GROUP BY three keys -> one wide shuffle produces the mart.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def main():
    spark = get_spark("gold_daily_revenue")

    # --- read silver, filtering & pruning as early as possible ---
    orders = (
        spark.read.parquet("data/silver/orders/")
        .filter(F.col("status") == "COMPLETE")            # only completed revenue counts
        .select("tenant_id", "order_id", "order_date")    # prune: only what we need downstream
    )
    items = (
        spark.read.parquet("data/silver/order_items/")
        .select("tenant_id", "order_id", "product_id", "item_revenue")
    )
    products = (
        spark.read.parquet("data/silver/products/")
        .select("tenant_id", "product_id", "category")
    )

    # --- build the fact: items is the spine ---
    fact = (
        items
        .join(orders, on=["tenant_id", "order_id"], how="inner")          # large-large (SMJ)
        .join(broadcast(products), on=["tenant_id", "product_id"], how="left")  # small (BHJ)
    )

    # --- aggregate to the mart grain: tenant x day x category ---
    daily_revenue = (
        fact.groupBy("tenant_id", "order_date", "category")
        .agg(
            F.sum("item_revenue").alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
            F.count("*").alias("items_sold"),
        )
    )

    (
        daily_revenue.write
        .mode("overwrite")
        .partitionBy("order_date")           # query-friendly: filter by date reads few folders
        .parquet("data/gold/daily_revenue/")
    )

    print("=== physical plan (compact) ===")
    daily_revenue.explain()                  # 'simple' mode: one-line tree, easy to read

    print("\n=== sample: top revenue (tenant, day, category) ===")
    (
        daily_revenue
        .orderBy(F.col("revenue").desc())
        .show(10, truncate=False)
    )
    print(f"mart rows: {daily_revenue.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()
