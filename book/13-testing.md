# Phase 13 — Testing & data contracts

**Code:** [`phase13-testing/`](../phases/phase13-testing/) —
`conftest.py`, `test_silver_orders.py`, `test_gold_revenue.py`

## Goal
Make jobs trustworthy with automated checks, so a bug or data drift blocks the pipeline
instead of quietly corrupting dashboards.

## Run
```bash
python -m pytest phases/phase13-testing/ -v
```

## The fixture
`conftest.py` provides a **session-scoped** SparkSession (starting Spark per test would
be painfully slow) and `chdir`s to the repo root so tests read `data/...`.

## The categories of check (all demonstrated)
| category | example test |
|---|---|
| **schema / completeness** | key columns are never null in silver |
| **validity** | `total_amount >= 0` (no negatives survive) |
| **uniqueness** | `(tenant_id, order_id)` is unique after dedup |
| **metric sanity** | silver row count within an expected band |
| **referential integrity** | (almost) every order_item has a parent order (`left_anti` join) |
| **metric reconciliation** | total mart revenue == independent recompute (float tolerance) |

## The pattern
```python
def test_no_negative_totals(spark):
    bad = spark.read.parquet(SILVER).filter(F.col("total_amount") < 0).count()
    assert bad == 0
```
A data-quality test is just: read the table, count violations, assert zero (or within a
tolerance band). Referential-integrity uses a `left_anti` join to count orphans; the
reconciliation test recomputes the headline metric a second, independent way.

## Success criteria
Automated checks for schema, completeness, validity, uniqueness, referential integrity,
and metric sanity — the contract silver/gold promise their consumers.
