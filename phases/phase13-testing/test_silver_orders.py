"""
test_silver_orders.py — data-quality contract for the silver orders table.

These are the invariants silver PROMISES downstream consumers. If any fail,
the silver job has a bug or the data drifted -- either way, block the pipeline.
"""

from pyspark.sql import functions as F

SILVER = "data/silver/orders/"


def test_no_null_keys(spark):
    """tenant_id / order_id / customer_id / order_date must never be null."""
    df = spark.read.parquet(SILVER)
    bad = df.filter(
        F.col("tenant_id").isNull() | F.col("order_id").isNull() |
        F.col("customer_id").isNull() | F.col("order_date").isNull()
    ).count()
    assert bad == 0, f"{bad} rows with null keys survived into silver"


def test_no_negative_totals(spark):
    """total_amount must be >= 0 (we quarantine negatives)."""
    df = spark.read.parquet(SILVER)
    assert df.filter(F.col("total_amount") < 0).count() == 0


def test_order_id_unique_per_tenant(spark):
    """(tenant_id, order_id) is the grain -> must be unique after dedup."""
    df = spark.read.parquet(SILVER)
    total = df.count()
    distinct = df.select("tenant_id", "order_id").distinct().count()
    assert total == distinct, f"{total - distinct} duplicate (tenant, order) keys remain"


def test_row_count_in_expected_range(spark):
    """sanity band: silver should retain most of the ~1M orders (scale=10)."""
    n = spark.read.parquet(SILVER).count()
    assert 900_000 <= n <= 1_010_000, f"unexpected silver order count: {n:,}"
