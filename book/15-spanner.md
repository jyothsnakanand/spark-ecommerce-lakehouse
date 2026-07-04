# Phase 15 — OLTP vs OLAP (the Spanner connection)

**Reference:** [`docs/architecture.md`](../docs/architecture.md)

## Goal
Understand where Spark fits in a real system — and where it must **not** go.

## The boundary
```
SPANNER (OLTP)  ──change stream/export──▶  Landing  ──▶  SPARK (OLAP)  ──▶  warehouse/BI/ML
system of record   Pub/Sub · Kafka · GCS      bronze→silver→gold + streaming
```
| | OLTP (Spanner) | OLAP (Spark) |
|---|---|---|
| workload | many tiny transactions | few huge scans/joins |
| consistency | strong, immediate | eventual (batch/stream lag) |
| latency | ms, customer-facing | seconds–minutes, analytical |
| source of truth? | **yes** | **no** — a derived replica |

## The key lesson
**Do not make Spark your transactional serving layer.**
- **Spanner** owns: transactions, strong consistency, customer-facing writes,
  low-latency point reads, source-of-truth records.
- **Spark** owns: large scans, large joins, batch processing, streaming aggregations,
  feature generation, analytics — over *exported/streamed copies* of the OLTP data.

**Change Data Capture** (Spanner change streams → Pub/Sub/Kafka/object storage) is the
bridge: it turns transactional mutations into an append-only feed that Spark ingests
into bronze. Analytics never touches the live transactional store.

## Full circle
This is exactly the medallion pipeline you built: the "landing" zone stood in for a
Spanner export, and everything downstream (bronze → silver → gold → features →
streaming) is the analytical plane that reads *copies*, never the system of record.

## The learning sequence, in one line each
0 mental model · 1 skewed data · 2 bronze/Parquet · 3 silver quality · 4 join strategies
· 5 marts/shuffle · 6 windows · 7 partition layout · 8 read the plan/UI · 9 tune skew &
spill · 10 incremental/idempotent · 11 streaming/watermarks · 12 leakage-safe features ·
13 data contracts · 14 runbooks/metrics · 15 keep OLTP and OLAP separate.
