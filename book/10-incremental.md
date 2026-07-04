# Phase 10 — Incremental processing & backfills

**Code:** [`gold_daily_revenue_incremental.py`](../phases/phase10-incremental/gold_daily_revenue_incremental.py),
[`footgun_demo.py`](../phases/phase10-incremental/footgun_demo.py)

## Goal
Stop overwriting everything. Process only what changed, idempotently.

## Parameterized date window + pruning
```python
.filter((col("order_date") >= start) & (col("order_date") <= end) & (col("status")=="COMPLETE"))
```
Because silver is partitioned by `order_date`, this becomes a **PartitionFilter** — a
one-day run reads one folder, not all 261. (A plain string literal prunes fine; no
`to_date` needed.)

## The crux: `partitionOverwriteMode = dynamic`
```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
mart.write.mode("overwrite").partitionBy("order_date").parquet(OUT)
```
This changes what `overwrite` *means* on a partitioned table:
- **static (DEFAULT)** → deletes the **entire target directory**, keeps only what you
  just wrote. Run a 3-day job in static mode → the other 258 partitions are gone.
- **dynamic** → replaces **only the partitions you produced**, leaves the rest untouched.

We proved it: after writing 3 dates, still **261 partitions**, control date's mtime
**unchanged**, target dates refreshed.

## Idempotency
Re-running the same window recomputes those partitions from the same silver source →
**byte-identical fingerprint** (rows + total revenue unchanged). This is the discipline
that would have prevented the Phase 1/5 "partial rerun mixed two generations" bug.

## The static footgun (demonstrated)
`footgun_demo.py` (on a throwaway copy) read-and-static-overwrote the same path. Result:
**0 partitions + a crash** — static deleted the whole directory first, then (lazily) tried
to read the source files it had just deleted → `FILE_NOT_EXIST`. Two lessons: static
overwrite nukes the whole table, and **never read-and-overwrite the same path**.

## The over-partitioning tax (bonus)
The 261-task `Stage 0` on each read was **parallel partition discovery** (listing all
261 folders), not data reading — triggered because >32 partition dirs exist. Another
vote against partitioning by day when days are tiny.
