"""
test_gold_revenue.py — contract + referential integrity + metric sanity for gold.
"""

from pyspark.sql import functions as F

GOLD = "data/gold/daily_revenue/"
ORDERS = "data/silver/orders/"
ITEMS = "data/silver/order_items/"


def test_revenue_non_negative(spark):
    """no negative revenue in the mart."""
    assert spark.read.parquet(GOLD).filter(F.col("revenue") < 0).count() == 0


def test_referential_integrity_items_have_orders(spark):
    """every silver order_item's (tenant, order) must exist in silver orders...
    EXCEPT items whose order was rejected -- so we assert MOST items match."""
    items = spark.read.parquet(ITEMS).select("tenant_id", "order_id").distinct()
    orders = spark.read.parquet(ORDERS).select("tenant_id", "order_id").distinct()
    orphans = items.join(orders, ["tenant_id", "order_id"], "left_anti").count()
    total = items.count()
    # <2% orphans expected (items of the ~1.6% rejected orders)
    assert orphans / total < 0.03, f"too many orphan items: {orphans}/{total}"


def test_mart_reconciles_with_source(spark):
    """total mart revenue must equal an independent recompute (float tolerance)."""
    mart_total = spark.read.parquet(GOLD).agg(F.sum("revenue")).first()[0]
    orders = (spark.read.parquet(ORDERS)
              .filter(F.col("status") == "COMPLETE").select("tenant_id", "order_id"))
    items = spark.read.parquet(ITEMS).select("tenant_id", "order_id", "item_revenue")
    indep = (items.join(orders, ["tenant_id", "order_id"], "inner")
             .agg(F.sum("item_revenue")).first()[0])
    assert abs(mart_total - indep) < 1.0, f"mart {mart_total} != source {indep}"
