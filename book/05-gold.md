# Phase 5 — Gold marts

**Code:** [`phases/phase05-gold/gold_revenue_by_tenant.py`](../phases/phase05-gold/gold_revenue_by_tenant.py),
[`gold_daily_revenue.py`](../phases/phase05-gold/gold_daily_revenue.py)

## Goal
Build business-facing aggregated tables from silver. Gold answers questions like
"revenue by tenant, day, and category?".

## groupBy = a wide transformation = a shuffle
```python
orders.filter(col("status")=="COMPLETE")        # NARROW: per-row, no movement
      .groupBy("tenant_id").agg(sum("total_amount").alias("revenue"))  # WIDE: shuffle
```
The plan shows `HashAggregate(partial_sum) → Exchange → HashAggregate(sum)`: each
partition pre-sums locally (**map-side combine**) so only ~10 partial sums per
partition cross the network, not all the rows. `import sum as _sum` avoids clashing
with Python's builtin.

## The three-way mart (filter & prune early)
`gold_daily_revenue` joins `items ⋈ orders ⋈ broadcast(products)` then groups by
`(tenant, date, category)`. Two habits before any join:
- **filter early** (drop non-COMPLETE orders first → fewer rows to shuffle),
- **prune early** (`select` only needed columns → fewer bytes shuffled).

Always **reconcile** a new mart against an independent recompute — we verified total
mart revenue equals the summed COMPLETE order-item revenue to the cent.

## The skew insight that surprises everyone
`tenant_mega_001` was ~60% of revenue, yet the skewed `groupBy(sum)` was **not slow** —
because `partial_sum` collapses each key to one number *before* the shuffle. **Skew
barely hurts a pre-aggregatable sum.** It hurts **joins** (which can't pre-aggregate),
which is why Phase 9's skew lab uses a join.

## Success criteria
You can explain why `groupBy` is wide, where the shuffle happens, why gold is
partitioned by `order_date`, and why marts should be query-friendly.
