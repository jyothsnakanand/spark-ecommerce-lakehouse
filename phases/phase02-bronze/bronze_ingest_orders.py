"""
bronze_ingest_orders.py — Phase 2: raw CSV -> typed Parquet (the "bronze" layer)

Bronze = "the raw data, but in a good file format, with a real schema".
We do NOT clean or filter here (that's silver's job). We only:
    1. read the raw CSV with an EXPLICIT schema (never infer in production),
    2. write it out as Parquet (columnar, typed, compressed).
"""

import sys
import os

# make "from _spark import ..." work no matter where we launch from
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType, DoubleType,
)

# ---------------------------------------------------------------------------
# EXPLICIT schema. We declare every column's name, type, and nullability
# ourselves instead of asking Spark to guess with inferSchema.
# ---------------------------------------------------------------------------
ORDERS_SCHEMA = StructType([
    StructField("tenant_id",      StringType(),    False),  # False = NOT nullable
    StructField("order_id",       StringType(),    False),
    StructField("customer_id",    StringType(),    True),
    StructField("order_ts",       TimestampType(), True),
    StructField("order_date",     DateType(),      True),
    StructField("status",         StringType(),    True),
    StructField("currency",       StringType(),    True),
    StructField("subtotal",       DoubleType(),    True),
    StructField("tax",            DoubleType(),    True),
    StructField("shipping",       DoubleType(),    True),
    StructField("total_amount",   DoubleType(),    True),
    StructField("payment_status", StringType(),    True),
])


def main():
    spark = get_spark("bronze_ingest_orders")

    orders_raw = (
        spark.read
        .schema(ORDERS_SCHEMA)          # apply OUR schema, no inference
        .option("header", "true")        # first CSV line is column names
        .csv("data/landing/orders/")
    )

    # write bronze as Parquet (overwrite = fully replace the folder each run)
    (
        orders_raw.write
        .mode("overwrite")
        .parquet("data/bronze/orders/")
    )

    # --- quick verification that reading it back gives the same rows ---
    bronze = spark.read.parquet("data/bronze/orders/")
    print(f"landing CSV rows : {orders_raw.count():,}")
    print(f"bronze parquet   : {bronze.count():,}")
    print("\nbronze schema (note the REAL types, not all-string):")
    bronze.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
