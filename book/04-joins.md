# Phase 4 — Joins & dimensional enrichment

**Code:** [`phases/phase04-joins/join_lab.py`](../phases/phase04-joins/join_lab.py)

## Goal
Join facts to dimensions correctly, and understand the two join *strategies*.

## The multi-tenant trap
```python
orders.join(customers, on="customer_id")               # WRONG — mixes tenants
orders.join(customers, on=["tenant_id","customer_id"]) # RIGHT — composite key
```
With two tenants each having a local `customer_id="1"`, joining on `customer_id` alone
matches every "1" to every other "1" → **4 rows instead of 2**, revenue attributed to
the wrong tenant. No error — just wrong numbers forever. **Always include `tenant_id`
in the key** so correctness never depends on ids happening to be globally unique.

## The LEFT-join invariant
A `left` join must **never change the left row count**. If `enriched.count() !=
orders.count()`, the right side has duplicate keys and is fanning out rows — a classic
silent bug. Assert it.

## The two strategies (read them in the plan)
```
SortMergeJoin                      BroadcastHashJoin  BuildRight
├─ Sort → Exchange (shuffle big)   ├─ Scan orders        ← NO shuffle
└─ Sort → Exchange (shuffle dim)   └─ BroadcastExchange(dim)
```
- **SortMergeJoin** (large ⋈ large): shuffles **both** sides by the key so matching
  keys co-locate — two `Exchange` nodes, expensive.
- **BroadcastHashJoin** (large ⋈ small): ships a full copy of the small table to every
  task; the **big table never moves**. `broadcast(df)` requests it; AQE also does it
  automatically for tables under `autoBroadcastJoinThreshold` (10MB default).

## Bonus in the plan
The dim scan shows `PushedFilters: [IsNotNull(...)]` — the not-null join-key check was
pushed into the Parquet read (predicate pushdown, automatic).

## Success criteria
You can explain when Spark shuffles for a join, why broadcast avoids it, why
`tenant_id` belongs in join keys, and why a wrong key corrupts analytics *silently*.
