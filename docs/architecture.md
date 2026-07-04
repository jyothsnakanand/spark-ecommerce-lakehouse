# Architecture — OLTP vs OLAP, and where Spark fits (Phase 15)

## The medallion lakehouse we built
```
landing (raw CSV/JSON)   ← synthetic generator (or a real system export)
      │  explicit schema, CSV/JSON → Parquet
      ▼
bronze  (raw, but typed & columnar)
      │  validate · dedupe · quarantine (rejection_reason)
      ▼
silver  (clean, trustworthy, deduplicated)
      │  joins · aggregations · windows
      ▼
gold    (business marts + ML features)
      │
      ▼
BI dashboards · ML models · reporting
```
Each layer has one job: **bronze** preserves raw data in a good format, **silver**
enforces the data-quality contract, **gold** answers business questions.

## OLTP vs OLAP — the system boundary (the Spanner connection)
This project maps onto a real production shape:

```
   ┌─────────────────────┐        change stream / export        ┌──────────────────┐
   │   SPANNER (OLTP)     │  ───────────────────────────────▶   │  Landing (files/  │
   │  system of record   │     Pub/Sub · Kafka · GCS/S3        │  Kafka topics)    │
   │  orders, payments,  │                                      └────────┬─────────┘
   │  customers, inventory│                                               │
   └─────────────────────┘                                               ▼
        ▲                                                        ┌──────────────────┐
        │ low-latency, strongly-consistent                       │   SPARK (OLAP)   │
        │ transactional reads/writes                             │ bronze→silver→   │
   customer-facing app                                           │ gold + streaming │
                                                                 └────────┬─────────┘
                                                                          ▼
                                                             warehouse / lakehouse
                                                             analytics · ML · BI
```

| | OLTP (Spanner) | OLAP (Spark) |
|---|---|---|
| workload | many tiny transactions | few huge scans/joins |
| consistency | strong, immediate | eventual (batch/stream lag) |
| latency | milliseconds, customer-facing | seconds–minutes, analytical |
| shape | normalized, row-oriented | denormalized marts, columnar |
| source of truth? | **yes** | **no** — a derived analytical replica |

## The key lesson
**Do not make Spark your transactional serving layer.**
- **Spanner** owns: transactions, strong consistency, customer-facing writes,
  low-latency point reads, the source-of-truth records.
- **Spark** owns: large scans, large joins, batch processing, streaming
  aggregations, feature generation, analytics — over *exported/streamed copies*
  of the OLTP data.

Change Data Capture (Spanner change streams → Pub/Sub/Kafka/object storage) is the
bridge: it turns transactional mutations into an append-only feed that Spark
ingests into bronze. Analytics never touches the live transactional store.
