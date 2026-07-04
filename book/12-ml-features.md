# Phase 12 — ML feature engineering

**Code:** [`gold_customer_features_ml.py`](../phases/phase12-ml-features/gold_customer_features_ml.py),
[`product_affinity.py`](../phases/phase12-ml-features/product_affinity.py)

## Goal
Build ML-ready feature tables — which are just another kind of gold table — with the
correctness properties ML needs.

## Point-in-time correctness & leakage
```python
orders.filter((col("status")=="COMPLETE") & (col("order_date") <= AS_OF))
```
Compute features using **only** data that existed by `AS_OF`. Using future orders to
predict the past is **leakage**: great offline metrics, then collapse in production.
Stamp `as_of_date` so features are reproducible "as of" a point in time.

## Time-windowed features (conditional aggregation)
```python
F.sum(F.when(col("days_ago") <= 7,  col("total_amount")).otherwise(0)).alias("revenue_7d")
F.sum(F.when(col("days_ago") <= 30, 1).otherwise(0)).alias("orders_30d")
```
Trailing 7d/30d windows computed in one pass — recency-weighted signal beats lifetime
totals for churn/propensity.

## Completeness (fix the Phase 6 gap)
```python
customers.join(agg, ["tenant_id","customer_id"], "left").na.fill(0, [...])
```
LEFT-join back to the full customer dimension so **non-buyers get a row of zeros**
instead of silently vanishing — models usually need a row for every entity.

## Product affinity via SELF-JOIN
```python
a.join(b, (a.tenant_id==b.tenant_id) & (a.order_id==b.order_id) & (a.product_id < b.product_id))
```
Join `order_items` to itself on the order to find product **pairs** bought together.
`a.product_id < b.product_id` keeps each unordered pair once (no self-pairs, no dupes).
**Danger:** an order with `k` items yields `k·(k-1)/2` pairs — quadratic. `k=1000` →
~500k pairs from one order. **Self-joins explode; always cap basket size.**

## Success criteria
You can explain why feature tables are gold tables, why `as_of_date` matters, why
leakage is dangerous, and why self-joins can blow up data size.
