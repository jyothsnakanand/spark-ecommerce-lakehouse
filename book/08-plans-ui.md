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

## Why one `count()` shows up as TWO jobs (AQE)
A `count()` isn't one step: each partition counts its own rows (`partial_count`), then
those partials shuffle to one place and sum. With **AQE on**, Spark runs the **map side
as its own job**, *stops to read the real shuffle statistics*, re-optimizes, then runs the
**reduce side as a second job**. So a query with one shuffle → two jobs; the second job's
"skipped" stages are the map output it **reused** instead of recomputing. Turn AQE off and
the same `count()` collapses to one job / two stages. (This is also why an AQE plan printed
before execution says `isFinalPlan=false` — it re-plans at each shuffle boundary at runtime.)

## Why PySpark job descriptions look like gibberish
The UI labels each job by the JVM call site that triggered the action. In PySpark your call
crosses the **Py4J bridge** into the JVM, so all the JVM sees is the reflection plumbing:
`csv/parquet/showString at NativeMethodAccessorImpl.java:0` (the *first word* — `csv`,
`parquet`, `showString`, `count` — is the useful part; the rest is noise). The
`$anonfun$…CompletableFuture.java:1768` entries are Spark's own async broadcast/AQE
housekeeping jobs, not code you wrote. In **Scala** Spark you'd see your real `Foo.scala:42`.
`(N skipped)` = stages/tasks whose results were reused from a prior shuffle — a *good* sign.

## Finding the heaviest stage (the method)
"Heaviest" isn't labelled — you read it off the columns: sort the **Stages** tab by
**Duration**, then corroborate with **Input / Shuffle Read / Shuffle Write** (data volume).
To hunt **skew**, open a **multi-task** stage (a 1/1 stage can't be skewed) and compare
**Max vs Median** in Summary Metrics. Also watch `Total Time Across All Tasks` vs the
stage `Duration`: if they're equal, tasks ran serially (no parallelism) — a red flag.

## Job descriptions & the interactive shell
`spark.sparkContext.setJobDescription("label")` stamps a readable name on subsequent jobs —
production code does this so the UI is navigable. To poke around live, launch the explorer
(`ipython -i phases/phase08-plans-ui/explore.py`): it preloads every table as a DataFrame +
SQL view, and the Spark UI stays up at `localhost:4040` for as long as the shell is alive.
