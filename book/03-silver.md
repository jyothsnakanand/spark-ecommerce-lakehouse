# Phase 3 — Silver cleaning & data quality

**Code:** [`phases/phase03-silver/`](../phases/phase03-silver/)

## Goal
Turn raw-but-typed bronze into **trustworthy** silver: validated, deduplicated, with
rejects quarantined (never silently deleted).

## The `rejection_reason` tag pattern (one pass, not `subtract`)
Instead of computing a separate "bad" set, **tag every row** in one pass:
```python
rejection_reason = (
    when(col("customer_id").isNull(), lit("null_customer"))
    .when((col("total_amount").isNull()) | (col("total_amount") < 0), lit("bad_total"))
    .otherwise(lit(None).cast("string")))          # null = passed all rules
tagged = orders.withColumn("rejection_reason", rejection_reason)
valid  = tagged.filter(col("rejection_reason").isNull())
```
A `when(...).when(...).otherwise(...)` chain is SQL `CASE WHEN`, evaluated top-to-bottom,
**first match wins**. `lit()` wraps a literal as a column. This is a **narrow** pass —
no shuffle — unlike the set-difference `subtract`.

## Deduplication — the window/`row_number` idiom
```python
w = Window.partitionBy("tenant_id","order_id").orderBy(col("order_ts").desc())
deduped = valid.withColumn("rn", row_number().over(w)).filter(col("rn")==1).drop("rn")
```
Number rows `1,2,3…` within each key, keep #1 (the latest). Unlike `dropDuplicates`,
this lets you choose *which* duplicate to keep. **Wide** (needs a shuffle + sort).

## Quarantine + reconcile
Rejects (validation failures + dedup losers, each tagged) are written to
`rejected_orders/` partitioned by reason. The report proves **airtight
reconciliation**: `silver + rejected == bronze`.

## Two subtleties we observed
- Rejected counts were slightly below what we injected — because **first-match tagging
  + overlapping dirt** (a row with two problems is counted once, under the earlier rule).
- We injected 2000 dirty events but rejected ~1589 — the **late-arriving** records are
  *valid* (just old dates), so they correctly stay in silver. Your generator's "dirty"
  and your cleaning's "bad" can legitimately disagree.

## Success criteria
A clean silver table, a reason-tagged quarantine table, a repeatable job, and a
written list of data-quality rules.
