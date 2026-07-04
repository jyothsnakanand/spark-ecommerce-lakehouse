"""
gold_top_products.py — Phase 6 Part B: window functions (top-N per group).

The key contrast:
  groupBy  -> collapses each group to ONE row (aggregation).
  window   -> computes across a group but KEEPS every row, adding a column
              (here: a rank of each product within its category).

Pipeline:
  1. groupBy to get revenue per product   (many items -> one row per product)
  2. WINDOW to rank products within (tenant, category)  (keeps every product row)
  3. filter to rank <= N
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

TOP_N = 5


def main():
    spark = get_spark("gold_top_products")

    items = spark.read.parquet("data/silver/order_items/") \
        .select("tenant_id", "product_id", "item_revenue")
    products = spark.read.parquet("data/silver/products/") \
        .select("tenant_id", "product_id", "product_name", "category")

    # 1. revenue per product (groupBy: one row per product)
    product_revenue = (
        items.join(broadcast(products), ["tenant_id", "product_id"], "inner")
        .groupBy("tenant_id", "category", "product_id", "product_name")
        .agg(F.sum("item_revenue").alias("revenue"))
    )

    # 2. WINDOW: rank products by revenue WITHIN each (tenant, category).
    #    partitionBy = the group; orderBy = the ranking order.
    w = Window.partitionBy("tenant_id", "category").orderBy(F.desc("revenue"))

    ranked = (
        product_revenue
        .withColumn("row_number", F.row_number().over(w))   # 1,2,3,4,... unique
        .withColumn("rank",       F.rank().over(w))          # ties share, gaps after
        .withColumn("dense_rank", F.dense_rank().over(w))    # ties share, no gaps
    )

    # 3. keep the top N per category (using row_number for a hard cap)
    top = ranked.filter(F.col("row_number") <= TOP_N)

    top.write.mode("overwrite").partitionBy("category").parquet("data/gold/top_products/")

    print(f"=== top {TOP_N} products per category, for tenant_mega_001 ===")
    (
        top.filter(F.col("tenant_id") == "tenant_mega_001")
        .select("category", "row_number", "product_name", "revenue")
        .orderBy("category", "row_number")
        .show(40, truncate=False)
    )
    spark.stop()


if __name__ == "__main__":
    main()
