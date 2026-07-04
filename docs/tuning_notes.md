# Tuning Notes (Phase 7–10 lessons, measured)

## Narrow vs wide
- **Narrow** (filter, project, withColumn): per-row, no data movement — cheap.
- **Wide** (groupBy, join, window, distinct): needs a **shuffle** (`Exchange`) —
  the expensive thing. Reading a plan = finding the shuffles.

## Shuffle partitions (`spark.sql.shuffle.partitions`)
It's a **U-curve** (measured on a 3.5M-row SortMergeJoin):
| partitions | time | why |
|---|---|---|
| 4 | 3000 ms | fat tasks, spill, only 4 cores used |
| 16 | 1293 ms | ~sweet spot |
| 200 (default) | 1848 ms | too many here |
| 1000 | 2418 ms | task overhead dominates |
| 1000 + **AQE on** | 1193 ms | AQE coalesced to the right number |
**Leave it high-ish and let AQE coalesce.** Default 200 is often wrong for small data.

## Spill
When a sort/aggregate doesn't fit execution memory it writes to disk. `Spill (Memory)`
= size as live objects; `Spill (Disk)` = serialized+compressed bytes actually written
(the ~5× gap is the compression ratio). Nonzero disk spill ⇒ give it more partitions
or more memory, or read less data.

## Joins
- large ⋈ small → **broadcast** the small side (`broadcast(df)`): no shuffle of the
  big table. Measured 5× faster (1613 → 313 ms). Guarded by `autoBroadcastJoinThreshold`
  (default 10MB) — must fit in memory.
- large ⋈ large → SortMergeJoin (shuffles both sides).

## Skew
- A pre-aggregatable `sum`/`count` on a skewed key is **fine** (map-side combine).
- A **join** on a skewed key is not: all of the hot key lands on one reduce task.
- Diagnose in the UI: heaviest stage → Summary Metrics → **Shuffle Read Max ÷ Median**.
  Skew lives on the shuffle-**read** (reduce) side.
- **Salting** splits the hot key across N sub-keys (scatter big side with 1 salt/row;
  replicate small side across all N salts; join on `(key, salt)`). But salting has
  real cost — in our test it made a *cheap* job **slower**. **Measure first**: skew
  only hurts when the imbalance sits on heavy per-key work. AQE's skew-join does this
  adaptively.

## Partitioning (physical layout)
- `partitionBy(col)` → one folder per value → **partition pruning** (`PartitionFilters`).
  Great for low-cardinality columns you filter by (date).
- Partitioning by high-cardinality `order_id` → ~1 folder per row = catastrophe
  (no pruning, thousands of tiny files, one task per file).
- `partitionBy` decides *which folder* (pruning); `repartition`/`coalesce` decides
  *how many files* per folder. Different knobs.
- Over-partitioning tax: >32 partition dirs triggers a parallel partition-discovery
  job on every read.

## Incremental writes
- `partitionOverwriteMode=dynamic` + `mode("overwrite")` replaces only the partitions
  you wrote. **Static (default) deletes the entire target directory** — a footgun.
- Never read-and-overwrite the same path (self-overwrite deletes the source mid-job).
