# Phase 6 — Window functions & customer analytics

**Code:** [`gold_customer_features.py`](../phases/phase06-windows/gold_customer_features.py),
[`gold_top_products.py`](../phases/phase06-windows/gold_top_products.py)

## The one sentence to remember
**`groupBy` collapses each group to one row; a window computes across the group but
keeps every row, adding a column.**

## Part A — customer LTV (a groupBy job)
Collapse many orders → one row per customer: `lifetime_revenue`, `lifetime_orders`,
`avg_order_value`, recency via `datediff(lit(AS_OF), col("last_order_date"))`.
Note two things we observed:
- The count came out **below** the customer count — customers with **no COMPLETE
  orders** silently drop from a `groupBy`. Fixing this "completeness gap" (left-join
  back to the customer dimension) matters for ML (Phase 12).
- Top customers spanned **all** tenants — the skew is in the *number* of customers per
  tenant, not in per-customer value. Knowing *which dimension* is skewed is a real skill.

## Part B — top-N per group (a window job)
```python
w = Window.partitionBy("tenant_id","category").orderBy(desc("revenue"))
ranked = product_revenue.withColumn("row_number", row_number().over(w))
top = ranked.filter(col("row_number") <= 5)
```
`partitionBy` = the group to rank within; `orderBy` = the ranking order. The DataFrame
keeps *every* product row and gains a rank column, then we filter to top 5.

### row_number vs rank vs dense_rank (ties on 100,100,90)
| | row_number | rank | dense_rank |
|---|---|---|---|
| values | 1,2,3 | 1,1,3 | 1,1,2 |
| use | hard "top N" cap | Olympic standings | count distinct tiers |

## Why windows can be expensive
`.over(w)` **shuffles** every row of a group onto one partition and **sorts** it —
and a window **cannot pre-aggregate** (unlike `sum`). So under skew, one group can
dominate a task (a Phase 9 concern).
