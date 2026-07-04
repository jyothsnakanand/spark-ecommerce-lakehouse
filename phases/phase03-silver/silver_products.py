"""
silver_products.py — Phase 3 pattern for the products dimension.

Contract: must have tenant_id, product_id, category.
Dedup on (tenant_id, product_id). Small dim table -> no partitioning on write.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql.functions import col, row_number, when, lit
from pyspark.sql.window import Window


def main():
    spark = get_spark("silver_products")
    products = spark.read.parquet("data/bronze/products/")
    total_in = products.count()

    rejection_reason = (
        when(col("tenant_id").isNull(),   lit("null_tenant"))
        .when(col("product_id").isNull(),  lit("null_product_id"))
        .when(col("category").isNull(),    lit("null_category"))
        .otherwise(lit(None).cast("string"))
    )
    tagged  = products.withColumn("rejection_reason", rejection_reason)
    invalid = tagged.filter(col("rejection_reason").isNotNull())
    valid   = tagged.filter(col("rejection_reason").isNull())

    w = Window.partitionBy("tenant_id", "product_id").orderBy(col("base_price").desc())
    ranked = valid.withColumn("rn", row_number().over(w))
    silver = ranked.filter(col("rn") == 1).drop("rn", "rejection_reason")
    dup_losers = ranked.filter(col("rn") > 1).drop("rn").withColumn("rejection_reason", lit("duplicate"))
    rejected = invalid.unionByName(dup_losers)

    silver.write.mode("overwrite").parquet("data/silver/products/")
    rejected.write.mode("overwrite").parquet("data/silver/rejected_products/")

    silver_cnt, rejected_cnt = silver.count(), rejected.count()
    print(f"bronze in      : {total_in:,}")
    print(f"silver out     : {silver_cnt:,}")
    print(f"rejected total : {rejected_cnt:,}")
    print(f"reconciles?    : {silver_cnt + rejected_cnt:,} == {total_in:,}  "
          f"({'YES' if silver_cnt + rejected_cnt == total_in else 'NO'})")
    spark.stop()


if __name__ == "__main__":
    main()
