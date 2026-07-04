"""
silver_customers.py — Phase 3 pattern, applied to customers.

Two differences worth noticing vs silver_orders:
  1. We didn't inject dirt into customers, so we expect ~0 rejects. That's fine
     -- a silver job still runs the SAME contract every time; "clean input"
     just means the contract passes. Idempotent and defensive by default.
  2. The dedup key is (tenant_id, customer_id) and we keep the LATEST profile
     by signup_date (a customer could appear twice if they re-registered).
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql.functions import col, row_number, when, lit
from pyspark.sql.window import Window


def main():
    spark = get_spark("silver_customers")

    customers = spark.read.parquet("data/bronze/customers/")
    total_in = customers.count()

    # 1. tag: a customer must have tenant_id and customer_id (its identity)
    rejection_reason = (
        when(col("tenant_id").isNull(),   lit("null_tenant"))
        .when(col("customer_id").isNull(), lit("null_customer_id"))
        .otherwise(lit(None).cast("string"))
    )
    tagged  = customers.withColumn("rejection_reason", rejection_reason)
    invalid = tagged.filter(col("rejection_reason").isNotNull())
    valid   = tagged.filter(col("rejection_reason").isNull())

    # 2. dedup: keep the latest profile per (tenant_id, customer_id)
    dedup_window = (
        Window.partitionBy("tenant_id", "customer_id").orderBy(col("signup_date").desc())
    )
    ranked = valid.withColumn("rn", row_number().over(dedup_window))

    silver_customers = ranked.filter(col("rn") == 1).drop("rn", "rejection_reason")
    dup_losers = (
        ranked.filter(col("rn") > 1).drop("rn").withColumn("rejection_reason", lit("duplicate"))
    )
    rejected_customers = invalid.unionByName(dup_losers)

    # writes: NOTE we do NOT partition customers by anything.
    # It's a small dimension table -- partitioning it would just make tiny files.
    silver_customers.write.mode("overwrite").parquet("data/silver/customers/")
    rejected_customers.write.mode("overwrite").parquet("data/silver/rejected_customers/")

    silver_cnt = silver_customers.count()
    rejected_cnt = rejected_customers.count()
    print(f"bronze in      : {total_in:,}")
    print(f"silver out     : {silver_cnt:,}")
    print(f"rejected total : {rejected_cnt:,}")
    print(f"reconciles?    : {silver_cnt + rejected_cnt:,} == {total_in:,}  "
          f"({'YES' if silver_cnt + rejected_cnt == total_in else 'NO'})")
    spark.stop()


if __name__ == "__main__":
    main()
