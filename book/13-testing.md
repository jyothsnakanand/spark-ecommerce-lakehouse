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

## Going deeper — a failing test that catches leakage ([`test_feature_ranges.py`](../phases/phase13-testing/test_feature_ranges.py))
Range/invariant checks on the feature table, aimed at `FEATURES_PATH` (env var):
- `days_since_last_order >= 0` (null allowed) — **negative recency = future leakage**,
- non-negative amounts,
- window **monotonicity**: `7d <= 30d <= lifetime` (a subset can't exceed its superset).

Run the *same* suite against the leaky vs correct feature table (TDD red→green):
- **RED** (leaky): `test_recency_never_negative FAILED — 90913 rows with NEGATIVE recency`.
  The other two passed — leakage corrupted *recency* only, so you need a **battery** of
  targeted invariants, not one catch-all.
- **GREEN** (correct point-in-time table): all pass.

Manual insight (Phase 12) → automated contract (Phase 13): if a refactor drops the
`order_date <= AS_OF` filter, CI goes red before the leaky table reaches a model.

## Success criteria
Automated checks for schema, completeness, validity, uniqueness, referential integrity,
and metric sanity — the contract silver/gold promise their consumers.
