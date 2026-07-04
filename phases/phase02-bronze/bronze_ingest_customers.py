"""
bronze_ingest_customers.py — Phase 2 pattern, applied to customers.

Same idea as bronze_ingest_orders: raw CSV -> typed Parquet, explicit schema,
no cleaning. Only the schema and paths differ.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql.types import StructType, StructField, StringType, DateType

CUSTOMERS_SCHEMA = StructType([
    StructField("tenant_id",   StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("name",        StringType(), True),
    StructField("email",       StringType(), True),
    StructField("country",     StringType(), True),
    StructField("signup_date", DateType(),   True),
])


def main():
    spark = get_spark("bronze_ingest_customers")

    customers_raw = (
        spark.read
        .schema(CUSTOMERS_SCHEMA)
        .option("header", "true")
        .csv("data/landing/customers/")
    )
    customers_raw.write.mode("overwrite").parquet("data/bronze/customers/")

    print(f"landing rows : {customers_raw.count():,}")
    print(f"bronze rows  : {spark.read.parquet('data/bronze/customers/').count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
