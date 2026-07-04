# Phase 8 — Plans & the Spark UI

**Code:** [`phases/phase08-plans-ui/plan_lab.py`](../phases/phase08-plans-ui/plan_lab.py),
[`explore.py`](../phases/phase08-plans-ui/explore.py)

## The four plan phases (`explain(mode="extended")`)
Every query passes through four representations:
1. **Parsed Logical** — literal transcription of your code; columns unresolved (`'`).
2. **Analyzed Logical** — columns/types resolved against the schema.
3. **Optimized Logical** — **Catalyst** rewrites: merges filters, infers `isnotnull`,
   **prunes columns** (inserts a `Project`), folds constants. *Your query gets faster
   here for free.*
4. **Physical** — concrete strategies (join type, exchanges, codegen); filters+pruning
   fused into the scan (`PushedFilters`, `ReadSchema` with only the needed columns).

## Counting stages from the plan
**#stages ≈ #shuffle-`Exchange` nodes + 1** (a stage is the run of narrow ops between
two shuffles). `BroadcastExchange` doesn't create a big-data shuffle stage.
Insight from the mart query: a single `groupBy` needed **two** Exchanges — because
`countDistinct` expands the shuffle key to include the distinct column first, then
re-shuffles to count. **`countDistinct` costs an extra shuffle stage.**

## The Spark UI (localhost:4040 while a job runs)
- **SQL/DataFrame tab** → the visual DAG of the plan.
- **Jobs tab** → AQE splits execution at each shuffle boundary, so one `count()` can be
  two jobs (map side, then reduce side — the "skipped" stages are reused shuffle output).
- **Stages tab** → the **heaviest stage** = largest Duration, corroborated by largest
  Input / Shuffle Read / Shuffle Write. Those columns *are* the diagnostic.
- **Summary Metrics** → compare **Max vs Median**. Equal = balanced. `Max ≫ Median` =
  **skew** (one task in the long tail). Skew only shows on **multi-task** stages, and
  lives on the shuffle-**read** (reduce) side.
- **Spill (Memory/Disk)** nonzero = a task ran out of memory and hit disk.

## Job descriptions
`spark.sparkContext.setJobDescription("label")` makes the UI readable (PySpark
otherwise labels jobs with the JVM call site `NativeMethodAccessorImpl.java:0`).
