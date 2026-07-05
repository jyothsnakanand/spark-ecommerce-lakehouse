# Appendix — conventions & gotchas

Cross-cutting things that bit us (or that are worth knowing) across the whole project.

## Environment
- **Python version matters.** PySpark lags new CPython releases — use **3.9–3.12**, not
  3.13/3.14 (py4j/cloudpickle break on brand-new Python). We pinned a `learn_spark` pyenv env.
- **Java 17 or 21.** Spark 4.x supports both; the `incubator.vector` WARN on Java 21 is Spark
  *using* SIMD acceleration — a good thing.
- **Harmless startup noise:** `Your hostname … resolves to a loopback address` and `Unable to
  load native-hadoop library` are Spark noticing your laptop isn't a Hadoop cluster. Ignore forever.
- **Run scripts from the repo root** — paths like `data/silver/orders/` are relative to it.
  Quiet the log firehose with `spark.sparkContext.setLogLevel("ERROR")` (done in `_spark.py`).

## Conventions
- **`from pyspark.sql import functions as F`** — the near-universal alias for the column-function
  toolbox (`F.col`, `F.sum`, `F.when`, `F.window`, …). Interactive code uses `F.` so you don't
  stop to import each function.
- **`sum as _sum`** — Spark's `sum`/`count`/`max` are *column* functions that clash with Python
  builtins. Alias them (or use `F.sum`) so it's unambiguous which you mean.
- **`lit()`** wraps a Python literal as a column; you need it wherever a column is expected
  (e.g. `F.datediff(F.lit("2026-07-04"), col("d"))`).
- **Composite keys everywhere** — multi-tenant ids are unique *within* a tenant, so every join /
  dedup / window key is `(tenant_id, <id>)`. Skipping `tenant_id` silently corrupts analytics.

## Gotchas that cost us time
- **`uuid4()` ignores `random.seed`.** It draws from OS entropy, so "reproducible" data wasn't —
  ids changed every run while counts stayed stable. Use seeded `random.getrandbits`. (Phase 1)
- **Partial pipeline reruns mix data generations.** Silver-from-gen-A joined to silver-from-gen-B
  gave *zero* overlap and an empty mart. Always rebuild the **full** pipeline after regenerating,
  and debug "empty join" by checking **distinct-key overlap** between the two sides. (Phase 5)
- **Background Python buffers stdout.** Long-running jobs launched in the background wrote *empty*
  log files until they exited — Python block-buffers stdout when piped. Run with **`python -u`**
  (or `PYTHONUNBUFFERED=1`) so console/streaming output flushes live. (Phase 9/11)
- **macOS `head -n -1` doesn't work** (BSD head has no negative counts). Use `sed '$d'` or a
  Python `lines[:-1]` to drop the last line. (Phase 14)
- **`nullable=False` in a schema is a weak hint**, not enforcement — Parquet round-trips come back
  nullable. Enforce not-null yourself in silver. (Phase 2)
- **Static partition overwrite deletes the whole table**, and reading+overwriting the same path
  self-destructs (delete happens before the lazy read). Use `partitionOverwriteMode=dynamic`. (Phase 10)

## Recurring ideas (the through-lines)
- **narrow vs wide / the shuffle** — the lens for all performance work.
- **the `row_number` window** — dedup (3), top-N ranking (6), CDC merge (15): one idiom, three jobs.
- **point-in-time correctness** — leakage (12) and CDC time-travel (15) are the same idea.
- **bands beat binary checks** — data-quality tests (13) and metric guards (14).
- **skew** — harmless in a pre-aggregated `sum` (5), brutal in a join (9), quadratic in a
  self-join (12). Same phenomenon, different masks.
- **measure before optimizing** — salting a non-bottleneck made it *slower* (9).
