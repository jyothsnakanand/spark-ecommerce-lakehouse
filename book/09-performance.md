# Phase 9 — Performance & skew tuning

**Code:** [`phase09-performance/`](../phases/phase09-performance/) —
`perf_lab_shuffle.py`, `spill_demo.py`, `perf_lab_broadcast.py`, `skew_lab.py`,
`salt_lab.py`, `salt_visual.py`

> Run these at scale: `python phases/phase01-data-generation/generate_data.py --scale 10`
> then rebuild the pipeline. scale=1 is too small to misbehave.

## Lab 1&2 — shuffle partitions (`perf_lab_shuffle.py`)
Same 3.5M-row SortMergeJoin, different partition counts (a **U-curve**):
| partitions | time | why |
|---|---|---|
| 4 | 3000 ms | fat tasks, spill, only 4 cores used |
| 16 | 1293 ms | sweet spot |
| 200 (default) | 1848 ms | too many here |
| 1000 | 2418 ms | task overhead |
| 1000 **+ AQE** | **1193 ms** | AQE coalesced to the right number |
**Leave it high and let AQE coalesce.** Correctness never changed — only speed.

> **Where does the partition count come from?** Not the data — the config. A plan node
> `Exchange hashpartitioning(tenant_id, 8)` means each row goes to `hash(tenant_id) % 8`,
> and the `8` is `spark.sql.shuffle.partitions` (Spark's default is **200**; `_spark.py`
> sets 8 for our small data). With AQE on, that number is just the *initial* target —
> AQE coalesces it at runtime based on real shuffle size (why the pre-execution plan says
> `isFinalPlan=false`).

## Spill (`spill_demo.py`)
Forced with `shuffle.partitions=2` + a low `numElementsForceSpillThreshold`. The UI
showed `Spill (Memory) 268 MiB`, `Spill (Disk) 52 MiB` — **same data, two measures**:
memory = live-object size, disk = serialized+compressed bytes (the ~5× gap is
compression). Balanced across tasks → this spill was from too-few-partitions, not skew.

## Lab 3 — broadcast (`perf_lab_broadcast.py`)
Same join, **1613 ms → 313 ms (5×)** by broadcasting the small side. The big table
never shuffles. Guarded by `autoBroadcastJoinThreshold` (must fit in memory).

## Lab 4 — manufacture skew (`skew_lab.py`)
Join on `tenant_id` (the skewed column), forced SortMergeJoin, AQE off. One reduce task
pulled **Shuffle Read Max 592,849 vs Median 19,631 = 30× the data** — mega's 60% on one
task. But **Duration was barely skewed** because `count` is cheap: skew's *pain* needs
heavy per-key work, not just data imbalance. Diagnose with **Shuffle Read Max ÷ Median**.

## Lab 5 — salting (`salt_lab.py`, `salt_visual.py`)
Split the hot key across N sub-keys: **scatter the big side** (1 random salt per row),
**replicate the small side across all N salts**, join on `(key, salt)`. Same result,
N-way spread. **But in our test salting made a *cheap* job SLOWER** (2135 → 3736 ms) —
because there was no real bottleneck to fix. **The meta-lesson: measure first.** Skew
only hurts with heavy per-key work / large data / spill. AQE's skew-join does this
adaptively — prefer it to hand-salting.

## The toolkit (with judgment)
| tool | when |
|---|---|
| shuffle.partitions / AQE coalesce | right-size tasks; avoid spill & tiny-task overhead |
| `broadcast()` | large ⋈ small |
| diagnosing skew | Shuffle Read Max ÷ Median on the reduce stage |
| salting / AQE skew-join | only when a skewed task is a real heavy bottleneck |
