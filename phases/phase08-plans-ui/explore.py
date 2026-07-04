"""
explore.py — an interactive scratchpad for poking at the lakehouse.

Launch it so Python STAYS interactive after loading (the -i flag):

    ipython -i jobs/explore.py        # nicest: tab-complete + history
    # or
    python  -i jobs/explore.py        # plain REPL, works fine too

After it loads you'll have these ready to use:
    spark                 the SparkSession
    orders_bronze         bronze orders  (raw-but-typed)
    orders                silver orders  (clean, deduped)
    rejected              quarantined rows (with rejection_reason)
    tenants, customers    raw landing CSVs

...and SQL views of the same names, so you can also do:
    spark.sql("select tenant_id, count(*) c from orders group by tenant_id order by c desc").show()
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from _spark import get_spark

# import common functions into the namespace so you can use them at the prompt
from pyspark.sql import functions as F  # e.g. F.col, F.sum, F.count, F.desc

spark = get_spark("explore")

# ---- load whatever exists; skip gracefully if a layer isn't built yet ----
def _load(path, reader):
    try:
        return reader(path)
    except Exception as e:
        print(f"  (skip {path}: {type(e).__name__})")
        return None

_csv = lambda p: spark.read.option("header", True).csv(p)

# silver (clean) tables
orders    = _load("data/silver/orders/",       spark.read.parquet)
customers = _load("data/silver/customers/",     spark.read.parquet)
items     = _load("data/silver/order_items/",   spark.read.parquet)
products  = _load("data/silver/products/",      spark.read.parquet)
rejected  = _load("data/silver/rejected_orders/", spark.read.parquet)

# gold (business) marts
daily_revenue     = _load("data/gold/daily_revenue/",     spark.read.parquet)
revenue_by_tenant = _load("data/gold/revenue_by_tenant/", spark.read.parquet)

# bronze + raw landing (for comparison/debugging)
orders_bronze = _load("data/bronze/orders/", spark.read.parquet)
tenants       = _load("data/landing/tenants/", _csv)

# register SQL views for anything that loaded
_registry = {
    "orders": orders, "customers": customers, "items": items, "products": products,
    "rejected": rejected, "daily_revenue": daily_revenue,
    "revenue_by_tenant": revenue_by_tenant, "orders_bronze": orders_bronze,
    "tenants": tenants,
}
for name, df in _registry.items():
    if df is not None:
        df.createOrReplaceTempView(name)

print("\n" + "=" * 64)
print("Loaded (DataFrames + SQL views):")
print("  silver: orders, customers, items, products, rejected")
print("  gold  : daily_revenue, revenue_by_tenant")
print("  raw   : orders_bronze, tenants")
print("Spark UI while this runs:  http://localhost:4040")
print("Try:")
print("  daily_revenue.orderBy(F.desc('revenue')).show(5)")
print("  items.join(products, ['tenant_id','product_id']).groupBy('category')\\")
print("       .agg(F.sum('item_revenue').alias('rev')).orderBy(F.desc('rev')).show()")
print("  spark.sql('select tenant_id, sum(revenue) r from daily_revenue "
      "group by 1 order by r desc').show()")
print("=" * 64 + "\n")
