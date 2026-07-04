"""
_spark.py — shared SparkSession factory used by every job.

One place to configure Spark so all jobs behave consistently. The leading
underscore is a Python convention meaning "internal helper, not a job you run
directly".
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Build (or fetch) a local SparkSession with sane defaults for this course."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")                         # executors = threads, 1 per core
        .config("spark.sql.adaptive.enabled", "true")  # AQE on (Phase 9 hero)
        .config("spark.sql.shuffle.partitions", "8")   # small data -> few shuffle parts
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")   # hush the INFO/WARN firehose
    return spark
