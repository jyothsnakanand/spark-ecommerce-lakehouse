"""bronze_ingest_products.py — Phase 2 pattern for products (raw CSV -> Parquet)."""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

PRODUCTS_SCHEMA = StructType([
    StructField("tenant_id",    StringType(), False),
    StructField("product_id",   StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category",     StringType(), True),
    StructField("base_price",   DoubleType(), True),
])


def main():
    spark = get_spark("bronze_ingest_products")
    products = (
        spark.read.schema(PRODUCTS_SCHEMA).option("header", "true")
        .csv("data/landing/products/")
    )
    products.write.mode("overwrite").parquet("data/bronze/products/")
    print(f"landing rows : {products.count():,}")
    print(f"bronze rows  : {spark.read.parquet('data/bronze/products/').count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
