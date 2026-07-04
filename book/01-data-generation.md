# Phase 1 — Generate realistic ecommerce data

**Code:** [`phases/phase01-data-generation/generate_data.py`](../phases/phase01-data-generation/generate_data.py)

## Goal
Create synthetic data you fully control — scale, skew, bad records, edge cases —
because those are exactly the things the rest of the course is about.

## The most important config: deliberate skew
```python
TENANTS = [("tenant_mega_001", 0.60), ("tenant_mid_001", 0.10), ...]
```
One mega-tenant owns **60%** of every table. Real multi-tenant SaaS looks like this,
and this single choice is what makes the skew labs (Phase 9) real instead of academic.
`weighted_tenant()` is a standard weighted random pick.

## The multi-tenant trap, baked into the data
`customer_id` is unique **within** a tenant, not globally. Orders reference real
customer ids of their own tenant. This forces every downstream join onto the
**composite key** `(tenant_id, customer_id)` — join on `customer_id` alone and you
silently mix tenants (Phase 4).

## Injected dirt (≈2% of orders)
`null_customer`, `null_date`, `negative_total`, exact `duplicate` rows, and
`late_arriving` records. This is the payload silver (Phase 3) has to clean. A
`dirty_log` counts each type so later we can assert silver caught exactly them.

## Reproducibility war story
The generator originally used `uuid.uuid4()` for ids. **`uuid4` draws from OS entropy
and ignores `random.seed`** — so row *counts* were reproducible but every *id* changed
each run. Combined with a partial pipeline rerun, this produced silver tables from two
different generations with **zero overlapping order ids** → an empty mart. The fix:
seeded `random.getrandbits`. The lesson: **verify determinism on the columns that
matter (the ids), and always rebuild the full pipeline after regenerating.**

## Success criteria
You can generate clean, bad, duplicate, late, skewed, high-cardinality,
date-partitionable data — reproducibly.

## What breaks at 100×
Pure-Python generation and holding millions of rows in a list gets memory-heavy.
Scale is a `--scale` flag; at cluster scale you'd generate with Spark itself.
