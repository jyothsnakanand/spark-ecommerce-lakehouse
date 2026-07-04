# Phase 7 — File layout & partitioning lab

**Code:** [`phases/phase07-partitioning/partition_lab.py`](../phases/phase07-partitioning/partition_lab.py)

## Goal
Feel — with measurements — why physical data layout decides query speed.

## The experiment (same data, three layouts)
| layout | dirs | files | avg file | one-day query |
|---|---|---|---|---|
| `partitionBy(order_date)` | 245 | 603 | 12 KB | **prunes** to 1 folder |
| `partitionBy(order_id)` (3k sample) | 3000 | 3000 | 3.2 KB | reads all 3000 |
| `repartition(order_date)` then `partitionBy` | 245 | 245 | 24 KB | prunes, tidy files |

## PartitionFilters vs PushedFilters
Filtering `order_date` on the date-partitioned table shows in the plan:
`PartitionFilters: [order_date = ...]` — Spark reads the value from the **folder name**
and skips the other folders entirely (free). Partitioning by `order_id` gives only
`PushedFilters` — Spark must **open every file** and check inside. The bad query
literally spawned **3000 tasks, one per tiny file** — pure scheduling overhead.

## Two different knobs (don't conflate them)
- **`partitionBy(col)`** decides *which folder* a row lands in → enables **pruning**.
  Good for low-cardinality columns you filter by (date). Toxic for high-cardinality
  (`order_id`) — one folder per value.
- **`repartition(col)` / `coalesce(n)`** decide *how many files* within that layout.
  `repartition("order_date")` puts all of a date's rows on one task → **one file per
  date** (603 → 245 files, and smaller total from better compression).

## The nuance that makes you dangerous
Even the "good" layout was **over-partitioned** — 245 partitions of ~12 KB. Partition
granularity must match data volume: partition by *day* only when a day is hundreds of
MB. Over-partitioning also taxes every read (>32 dirs triggers a parallel
partition-discovery job — see Phase 10).

## Success criteria
Explain why date partitioning prunes, why `order_id` partitioning is catastrophic,
why tiny files hurt (task overhead), and that partitioning is a *physical* design
decision separate from file sizing.
