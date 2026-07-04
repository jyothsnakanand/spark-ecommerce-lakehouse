"""
join_lab.py — Phase 4: joins, the multi-tenant trap, and join strategies.

Three demos:
  1. THE TRAP   - joining on customer_id alone silently corrupts multi-tenant data.
  2. THE FIX    - joining on the composite key (tenant_id, customer_id).
  3. STRATEGY   - SortMergeJoin (shuffle both sides) vs BroadcastHashJoin (ship
                  the small table to every task, no shuffle of the big one).
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def demo_1_the_trap(spark):
    print("\n" + "=" * 70)
    print("DEMO 1: the multi-tenant join trap (crafted tiny data)")
    print("=" * 70)
    # Two tenants that BOTH have a local customer id '1' -- different people.
    orders = spark.createDataFrame(
        [("A", "1", 100.0), ("B", "1", 999.0)],
        ["tenant_id", "customer_id", "amount"],
    )
    customers = spark.createDataFrame(
        [("A", "1", "Alice"), ("B", "1", "Bob")],
        ["tenant_id", "customer_id", "name"],
    )

    print("\nWRONG: join on customer_id ALONE ->")
    wrong = orders.join(customers, on="customer_id", how="inner")
    wrong.show(truncate=False)
    print(f"  rows: {wrong.count()}  (expected 2 -- got MORE = cross-tenant contamination)")

    print("\nRIGHT: join on the composite key (tenant_id, customer_id) ->")
    right = orders.join(customers, on=["tenant_id", "customer_id"], how="inner")
    right.show(truncate=False)
    print(f"  rows: {right.count()}  (each order matched exactly its own tenant's customer)")


def demo_2_real_join(spark):
    print("\n" + "=" * 70)
    print("DEMO 2: real silver join on composite key (row count must be preserved)")
    print("=" * 70)
    orders = spark.read.parquet("data/silver/orders/")
    customers = spark.read.parquet("data/silver/customers/")

    enriched = orders.join(customers, on=["tenant_id", "customer_id"], how="left")
    print(f"  silver orders : {orders.count():,}")
    print(f"  after LEFT join: {enriched.count():,}  "
          "(LEFT join must NOT change the order count)")


def demo_3_strategies(spark):
    print("\n" + "=" * 70)
    print("DEMO 3: SortMergeJoin vs BroadcastHashJoin (read the plans)")
    print("=" * 70)
    orders = spark.read.parquet("data/silver/orders/")
    customers = spark.read.parquet("data/silver/customers/")

    # (a) FORCE a shuffle join by disabling auto-broadcast, so we can see SMJ.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    smj = orders.join(customers, on=["tenant_id", "customer_id"], how="left")
    print("\n(a) autoBroadcast OFF  ->  expect SortMergeJoin + two Exchanges:")
    smj.explain("formatted")

    # (b) explicit broadcast of the small customers table.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # back to 10MB default
    bhj = orders.join(broadcast(customers), on=["tenant_id", "customer_id"], how="left")
    print("\n(b) broadcast(customers)  ->  expect BroadcastHashJoin, NO shuffle of orders:")
    bhj.explain("formatted")


def main():
    spark = get_spark("join_lab")
    demo_1_the_trap(spark)
    demo_2_real_join(spark)
    demo_3_strategies(spark)
    spark.stop()


if __name__ == "__main__":
    main()
