# Phase 2 — Bronze ingestion (raw CSV → typed Parquet)

**Code:** [`phases/phase02-bronze/`](../phases/phase02-bronze/) · shared session helper `_spark.py`

## Goal
Convert inefficient raw files into a typed, columnar analytical format. Bronze keeps
data **raw** (no cleaning) but in a good shape.

## Explicit schema — the Phase 2 lesson
```python
ORDERS_SCHEMA = StructType([StructField("total_amount", DoubleType(), True), ...])
orders = spark.read.schema(ORDERS_SCHEMA).option("header","true").csv("data/landing/orders/")
```
Never use `inferSchema` in production:
1. **Inference costs an extra full pass** to guess types.
2. **Guesses drift** — if a column happened to be all whole numbers Spark might infer
   `Integer` and silently break downstream math. An explicit schema is a **contract**.
3. `nullable=False` documents invariants — though note it's a weak hint (Parquet
   round-trips always come back nullable; enforce not-null yourself in silver).

## Read → write → verify
```python
orders.write.mode("overwrite").parquet("data/bronze/orders/")
```
Point readers at the **folder**, not a file (that's how Spark scales to thousands of
files). `mode("overwrite")` makes the job **idempotent**.

## Why Parquet beats CSV (we measured 12 MB → 4.5 MB)
Columnar (**column pruning** — read only the columns you select), typed (no
re-parsing), compressed, and carries per-chunk min/max stats enabling **predicate
pushdown** (skip blocks that can't match a filter). CSV has none of this.

## The output folder anatomy
`part-00000…` files (one per writing partition), a `_SUCCESS` marker Spark writes
*last* (its presence = "write finished cleanly"), and `.crc` checksums.

## Success criteria
You understand why explicit schemas beat inference, why Parquet beats CSV for
analytics, why bronze stays raw, and why nothing runs until an action.
