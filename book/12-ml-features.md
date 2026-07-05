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

## Going deeper — leakage, measured ([`leakage_demo.py`](../phases/phase12-ml-features/leakage_demo.py))
Compute "recency" the leaky way (max over ALL orders) vs correct (max over
`order_date <= AS_OF`), with a label = "bought in next 30 days":
- **90.9%** of leaky recencies were **negative** (last order "in the future") — impossible
  values are a leakage smoking gun.
- Mean recency by label: **correct = 14.1 for both classes** (no real signal), but
  **leaky = −13.8 vs −29.6** — the leaky feature separates the label because it *is*
  derived from the future purchase that defines the label. Great offline, dead online.
- Defenses: point-in-time cut, stamp `as_of_date`, and **split train/test by time**.

## Going deeper — self-join explosion ([`self_join_explosion.py`](../phases/phase12-ml-features/self_join_explosion.py))
Self-join output = `Σ k·(k-1)/2` over baskets — **quadratic**, so cost concentrates in the
biggest baskets (skew). A single 1000-item order adds ~499,500 pairs (~20% on top of 1M
real orders). You can **predict** the blowup from `groupBy(order).count()` without running
the join, and a `filter(k <= CAP)` is cheap insurance that's a no-op on clean data.

## Success criteria
You can explain why feature tables are gold tables, why `as_of_date` matters, why
leakage is dangerous, and why self-joins can blow up data size.
