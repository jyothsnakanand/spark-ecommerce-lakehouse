"""
test_feature_ranges.py — Phase 13: range & invariant checks on a feature table.

These are the contracts an ML feature table must satisfy. The recency check is the
one the Phase 12 leakage demo motivated: 'days since last order' can never be
negative -- a negative value means the feature peeked into the future (leakage).

The table under test is FEATURES_PATH (default = the correct point-in-time table).
Point it at a leaky table and these tests turn RED -- that's the guardrail working.
"""

import os
from pyspark.sql import functions as F

FEATURES = os.environ.get("FEATURES_PATH", "data/gold/customer_features_ml/")


def test_recency_never_negative(spark):
    """days_since_last_order >= 0 (null allowed for non-buyers). Negative == leakage."""
    df = spark.read.parquet(FEATURES)
    bad = df.filter(F.col("days_since_last_order") < 0).count()
    assert bad == 0, f"{bad} rows with NEGATIVE recency -> future leakage"


def test_no_negative_amounts(spark):
    df = spark.read.parquet(FEATURES)
    bad = df.filter((F.col("revenue_7d") < 0) | (F.col("revenue_30d") < 0) |
                    (F.col("lifetime_revenue") < 0) | (F.col("orders_7d") < 0)).count()
    assert bad == 0


def test_windows_are_monotonic(spark):
    """7d activity <= 30d activity <= lifetime (a subset can't exceed its superset)."""
    df = spark.read.parquet(FEATURES)
    bad = df.filter(
        (F.col("revenue_7d") > F.col("revenue_30d") + 0.001) |
        (F.col("revenue_30d") > F.col("lifetime_revenue") + 0.001) |
        (F.col("orders_7d") > F.col("orders_30d")) |
        (F.col("orders_30d") > F.col("lifetime_orders"))
    ).count()
    assert bad == 0, f"{bad} rows violate 7d <= 30d <= lifetime monotonicity"
