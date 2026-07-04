# Phase 0 — Setup & mental model

**Code:** [`phases/phase00-setup/smoke.py`](../phases/phase00-setup/smoke.py)

## Goal
Get Spark running locally and internalize the execution model before writing any
real jobs.

## The architecture in one paragraph
`SparkSession.builder…getOrCreate()` starts a **driver** — a JVM process that holds
your program and builds query plans. `master("local[*]")` runs the **executors** as
threads inside that same process, one per CPU core (`[*]`). On a cluster, executors
are separate machines. The driver *plans*; the executors *do the work in parallel*.

## Transformations vs. actions (the single most important idea)
```python
df = spark.range(0, 1000)          # transformation → builds a plan, runs NOTHING
df.filter("id % 2 = 0")            # transformation → still nothing
result.collect()                   # ACTION → NOW Spark executes
```
Every call returning a new DataFrame just appends a node to a plan (a DAG). Nothing
runs until an **action** (`collect`, `count`, `show`, `write`) demands a result.
Laziness lets Spark see the *whole* plan and optimize before executing.

## Reading the physical plan
`result.explain("formatted")` on the even-count job prints (read bottom-up):
```
Range → Filter → Project → HashAggregate(partial_count) → Exchange → HashAggregate(count)
        \_______ one fused stage, no data movement _______/    ⬆ shuffle
```
- **`Exchange` = shuffle** = data moving across partitions/network. **This is the
  expensive operation in all of Spark.** Everything in later phases is about seeing,
  minimizing, or surviving shuffles.
- Spark pre-aggregates each partition (`partial_count`) *before* the shuffle, so it
  ships 12 tiny numbers instead of 500 rows — a "map-side combine."
- `AdaptiveSparkPlan … isFinalPlan=true` means **AQE** (Adaptive Query Execution)
  re-optimized the plan at runtime using real statistics. Star of Phase 9.

## Success criteria
You can explain: why transformations are lazy, which action triggered the job, what
the physical plan shows, and where tasks run in parallel (the `splits`).

## What breaks at 100×
Nothing here — but the shuffle you just met is the thing that will. Hold onto
"narrow = cheap, wide = shuffle = expensive."
