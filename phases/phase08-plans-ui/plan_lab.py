"""
plan_lab.py — Phase 8: reading plans like an engineer.

Two parts:
  PART 1  a SIMPLE query with explain(mode="extended") -> see the 4 plan phases
          (Parsed -> Analyzed -> Optimized LOGICAL, then PHYSICAL) and watch the
          Catalyst optimizer rewrite your query (pushdown + column pruning).
  PART 2  the REAL 3-way-join mart query, and how to COUNT stages from the plan:
          #stages = #Exchanges (+ broadcast/result boundaries) + 1.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def part1_logical_vs_physical(spark):
    print("\n" + "#" * 72)
    print("# PART 1: explain(mode='extended') — the 4 plan phases on a SIMPLE query")
    print("#" * 72)
    q = (
        spark.read.parquet("data/silver/orders/")
        .filter(F.col("status") == "COMPLETE")
        .filter(F.col("total_amount") > 100)
        .groupBy("tenant_id")
        .agg(F.sum("total_amount").alias("revenue"))
    )
    # extended = Parsed Logical, Analyzed Logical, Optimized Logical, Physical
    q.explain(mode="extended")


def part2_count_the_stages(spark):
    print("\n" + "#" * 72)
    print("# PART 2: the mart query — find scans, shuffles, join strategy, #stages")
    print("#" * 72)
    orders = (spark.read.parquet("data/silver/orders/")
              .filter(F.col("status") == "COMPLETE")
              .select("tenant_id", "order_id", "order_date"))
    items = (spark.read.parquet("data/silver/order_items/")
             .select("tenant_id", "order_id", "product_id", "item_revenue"))
    products = (spark.read.parquet("data/silver/products/")
                .select("tenant_id", "product_id", "category"))

    mart = (
        items
        .join(orders, ["tenant_id", "order_id"], "inner")
        .join(broadcast(products), ["tenant_id", "product_id"], "left")
        .groupBy("tenant_id", "order_date", "category")
        .agg(F.sum("item_revenue").alias("revenue"),
             F.countDistinct("order_id").alias("orders"))
    )
    mart.explain(mode="formatted")


def main():
    spark = get_spark("plan_lab")
    part1_logical_vs_physical(spark)
    part2_count_the_stages(spark)
    spark.stop()


if __name__ == "__main__":
    main()
