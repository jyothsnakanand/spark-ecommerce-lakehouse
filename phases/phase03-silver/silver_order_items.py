"""
silver_order_items.py — Phase 3 pattern for order_items.

Contract for a line item:
  - must have tenant_id, order_id, order_item_id, product_id
  - quantity > 0
  - item_revenue >= 0
Dedup on the natural grain (tenant_id, order_item_id).
(No dirt was injected into items, so expect ~0 rejects -- contract still runs.)
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql.functions import col, row_number, when, lit
from pyspark.sql.window import Window


def main():
    spark = get_spark("silver_order_items")
    items = spark.read.parquet("data/bronze/order_items/")
    total_in = items.count()

    rejection_reason = (
        when(col("tenant_id").isNull(),     lit("null_tenant"))
        .when(col("order_id").isNull(),      lit("null_order_id"))
        .when(col("order_item_id").isNull(), lit("null_item_id"))
        .when(col("product_id").isNull(),    lit("null_product_id"))
        .when((col("quantity").isNull()) | (col("quantity") <= 0), lit("bad_quantity"))
        .when((col("item_revenue").isNull()) | (col("item_revenue") < 0), lit("bad_revenue"))
        .otherwise(lit(None).cast("string"))
    )
    tagged  = items.withColumn("rejection_reason", rejection_reason)
    invalid = tagged.filter(col("rejection_reason").isNotNull())
    valid   = tagged.filter(col("rejection_reason").isNull())

    w = Window.partitionBy("tenant_id", "order_item_id").orderBy(col("item_revenue").desc())
    ranked = valid.withColumn("rn", row_number().over(w))
    silver = ranked.filter(col("rn") == 1).drop("rn", "rejection_reason")
    dup_losers = ranked.filter(col("rn") > 1).drop("rn").withColumn("rejection_reason", lit("duplicate"))
    rejected = invalid.unionByName(dup_losers)

    silver.write.mode("overwrite").parquet("data/silver/order_items/")
    rejected.write.mode("overwrite").parquet("data/silver/rejected_order_items/")

    silver_cnt, rejected_cnt = silver.count(), rejected.count()
    print(f"bronze in      : {total_in:,}")
    print(f"silver out     : {silver_cnt:,}")
    print(f"rejected total : {rejected_cnt:,}")
    print(f"reconciles?    : {silver_cnt + rejected_cnt:,} == {total_in:,}  "
          f"({'YES' if silver_cnt + rejected_cnt == total_in else 'NO'})")
    spark.stop()


if __name__ == "__main__":
    main()
