"""
footgun_demo.py — Phase 10: what STATIC overwrite mode does to a partitioned
table (run on a THROWAWAY copy so we don't hurt real gold).

We take a copy of the mart, then write just 3 dates back into it with the
DEFAULT (static) partitionOverwriteMode. Static + overwrite deletes the WHOLE
target directory first -> every other date partition is destroyed.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark
from pyspark.sql import functions as F

COPY = "data/lab_footgun/daily_revenue"


def count_parts(spark):
    return spark.read.parquet(COPY).select("order_date").distinct().count()


def main():
    spark = get_spark("footgun_demo")
    # DEFAULT mode is 'static' -- we set it explicitly to be loud about it.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    print(f"copy has {count_parts(spark)} date partitions BEFORE")

    # grab just 3 dates out of the copy, then write them back with STATIC overwrite
    three = (spark.read.parquet(COPY)
             .filter(F.col("order_date").isin("2026-06-28", "2026-06-29", "2026-06-30")))
    (three.write.mode("overwrite").partitionBy("order_date").parquet(COPY))

    print(f"copy has {count_parts(spark)} date partitions AFTER  <-- static overwrite nuked the rest")
    spark.stop()


if __name__ == "__main__":
    main()
