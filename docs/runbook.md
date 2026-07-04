# Runbook — `gold_daily_revenue` (Phase 14)

A runbook is the on-call answer sheet for a job. Every production job should have one.

## What does this job do?
Builds the daily revenue mart: revenue by **tenant × day × category**, from the
silver `orders`, `order_items`, and `products` tables.

## Inputs
| table | path | grain |
|---|---|---|
| silver orders | `data/silver/orders/` | one row per (tenant, order) |
| silver order_items | `data/silver/order_items/` | one row per line item |
| silver products | `data/silver/products/` | one row per (tenant, product) |

## Output
- `data/gold/daily_revenue/`, partitioned by `order_date`.
- Columns: `tenant_id, order_date, category, revenue, orders, items_sold`.

## Schedule & runtime
- Runs daily after silver completes.
- Expected runtime: seconds (scale=10, local). Minutes-to-tens-of-minutes at cluster scale.

## How to run
```bash
# full rebuild
python phases/phase05-gold/gold_daily_revenue.py

# incremental (only affected dates) — PREFERRED for daily runs
python phases/phase10-incremental/gold_daily_revenue_incremental.py \
    --start-date 2026-07-03 --end-date 2026-07-03
```

## How to backfill
```bash
# 7-day backfill
python phases/phase10-incremental/gold_daily_revenue_incremental.py \
    --start-date 2026-06-01 --end-date 2026-06-07
# 30-day rebuild: widen the window. Full rebuild: run the phase05 job.
```
The incremental job uses `partitionOverwriteMode=dynamic`, so a backfill rewrites
**only** the dates in the window and leaves all other partitions untouched. It is
idempotent — re-running the same window produces identical output.

## Common failures
| symptom | likely cause | fix |
|---|---|---|
| empty mart / 0 rows | source tables from different data generations (key mismatch) | rebuild the **full** pipeline from one generation |
| `FILE_NOT_EXIST` mid-write | read-and-static-overwrite the same path | never self-overwrite; use dynamic mode / new path |
| one very slow task | skew on a join key | broadcast the small side, or salt (only if it's a real bottleneck) |
| tiny files / slow planning | over-partitioning | `repartition(col)` before write; coarser partition grain |

## How to validate output
```bash
python -m pytest phases/phase13-testing/test_gold_revenue.py -v
```
Checks: revenue ≥ 0, referential integrity (items → orders), and that total mart
revenue reconciles with an independent recompute from source.

## Ownership
- Source data owner: (the OLTP / Spanner export team — see `architecture.md`).
- Output consumers: BI dashboards, finance reporting, the ML feature jobs.

## Job metrics worth capturing
input rows, output rows, rejected rows, runtime seconds, shuffle read/write bytes,
number of output files. Emit these per run so regressions (e.g. a sudden row-count
drop, or file-count explosion) are visible before a human notices broken dashboards.
