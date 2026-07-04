"""
silver_orders.py — Phase 3: bronze -> silver (clean, deduplicated, trustworthy)

Bronze keeps raw data. Silver enforces the rules. Instead of computing a
separate "bad" set with subtract(), we TAG every row in one pass with a
`rejection_reason` column:
    - rejection_reason IS NULL      -> the row is good -> silver
    - rejection_reason IS NOT NULL  -> the row is bad  -> rejected (with reason)

This gives airtight accounting: bronze rows == silver + rejected, and every
rejected row tells you WHY it was rejected.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql.functions import col, row_number, when, lit
from pyspark.sql.window import Window


def main():
    spark = get_spark("silver_orders")

    orders = spark.read.parquet("data/bronze/orders/")
    total_in = orders.count()

    # ------------------------------------------------------------------
    # 1. TAG each row with the FIRST rule it violates (null = passes all).
    #    A when(...).when(...).otherwise(...) chain is Spark's CASE WHEN:
    #    it stops at the first matching branch, top to bottom.
    # ------------------------------------------------------------------
    rejection_reason = (
        when(col("tenant_id").isNull(),   lit("null_tenant"))
        .when(col("order_id").isNull(),   lit("null_order_id"))
        .when(col("customer_id").isNull(), lit("null_customer"))
        .when(col("order_date").isNull(),  lit("null_date"))
        .when(col("total_amount").isNull() | (col("total_amount") < 0), lit("bad_total"))
        .otherwise(lit(None).cast("string"))   # passed every rule -> no reason
    )
    tagged = orders.withColumn("rejection_reason", rejection_reason)

    invalid = tagged.filter(col("rejection_reason").isNotNull())
    valid   = tagged.filter(col("rejection_reason").isNull())

    # ------------------------------------------------------------------
    # 2. DEDUPLICATE the valid rows. Keep the latest per (tenant_id, order_id);
    #    tag the losers as "duplicate" so they are quarantined, not vanished.
    # ------------------------------------------------------------------
    dedup_window = (
        Window.partitionBy("tenant_id", "order_id").orderBy(col("order_ts").desc())
    )
    ranked = valid.withColumn("rn", row_number().over(dedup_window))

    silver_orders = ranked.filter(col("rn") == 1).drop("rn", "rejection_reason")
    dup_losers    = (
        ranked.filter(col("rn") > 1)
        .drop("rn")
        .withColumn("rejection_reason", lit("duplicate"))
    )

    # ------------------------------------------------------------------
    # 3. rejected = validation failures + duplicate losers.
    #    unionByName lines columns up by NAME (safer than union's positional).
    # ------------------------------------------------------------------
    rejected_orders = invalid.unionByName(dup_losers)

    # ----------------------------- writes -----------------------------
    (
        silver_orders.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet("data/silver/orders/")
    )
    (
        rejected_orders.write
        .mode("overwrite")
        .partitionBy("rejection_reason")   # one folder per reason -> easy debugging
        .parquet("data/silver/rejected_orders/")
    )

    # ----------------------------- report -----------------------------
    silver_cnt = silver_orders.count()
    rejected_cnt = rejected_orders.count()
    print(f"bronze in        : {total_in:,}")
    print(f"silver out       : {silver_cnt:,}")
    print(f"rejected total   : {rejected_cnt:,}")
    print(f"reconciles?      : {silver_cnt + rejected_cnt:,} == {total_in:,}  "
          f"({'YES' if silver_cnt + rejected_cnt == total_in else 'NO'})")
    print("\nrejected breakdown by reason:")
    rejected_orders.groupBy("rejection_reason").count().orderBy("rejection_reason").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
