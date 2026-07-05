# Phase 11 — Structured Streaming

**Code:** [`generate_clickstream.py`](../phases/phase11-streaming/generate_clickstream.py),
[`streaming_click_metrics.py`](../phases/phase11-streaming/streaming_click_metrics.py)

## Goal
Process an **unbounded** input: files arriving continuously, with Spark running the
*same* DataFrame logic **incrementally** as data lands.

## Run it
```bash
python phases/phase11-streaming/generate_clickstream.py --files 3 --events 400
python phases/phase11-streaming/streaming_click_metrics.py
```

## The pieces
```python
events = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger",1).json(STREAM_DIR)
per_min = (events.withWatermark("event_ts","2 minutes")
                 .groupBy(window("event_ts","1 minute"), "event_type").count())
per_min.writeStream.format("console").outputMode("update")
       .option("checkpointLocation", CP).trigger(availableNow=True).start()
```
- **`readStream`** — same API, infinite input. **Schema is mandatory** for file streams.
- **`window("event_ts","1 minute")`** — buckets by **event time** (when it happened),
  not processing time (when Spark saw it).
- **`withWatermark("event_ts","2 minutes")`** — "accept data up to 2 min late; once
  event-time passes `window_end + 2min`, finalize the window and drop its state." This
  is what keeps streaming aggregation state **bounded** and defines what "late" means.
- **`checkpointLocation`** — not optional. Records consumed files/offsets + in-flight
  state → **fault tolerance** + **incremental reruns** (already-seen files skipped).
- **`trigger(availableNow=True)`** — drain available files in micro-batches, then stop
  (reproducible instead of running forever).

## What we observed
- **Micro-batches:** 3 files → Batch 0/1/2 (`maxFilesPerTrigger=1`).
- **State accumulates:** window `12:00 view` grew 12 → 32 across batches.
- **Watermark eviction:** by Batch 2, windows `12:00–12:06` were finalized and dropped;
  Batch 2's events landing in them were **discarded as too-late**.
- **Checkpoint:** re-running processed **0** files (all seen); dropping one new file
  resumed at **Batch 3** — resumable, exactly-once.

## Output modes
**append** (only finalized rows, needs watermark) · **update** (changed rows) ·
**complete** (whole result table each batch).

## Going deeper — continuous execution
[`streaming_click_metrics_live.py`](../phases/phase11-streaming/streaming_click_metrics_live.py)
runs the query **forever** (`awaitTermination`) with a `trigger(processingTime="4 seconds")`
instead of `availableNow`. Feed it with `generate_clickstream.py --now --out <dir>` (the
`--now` flag timestamps events at wall-clock so windows advance). You watch batches fire
on the clock — empty when idle, updating counts within seconds when a file drips in, and
finalizing/evicting old windows as event-time passes the watermark. (Run the stream with
`python -u` so console output isn't buffered.)

| | `availableNow` | continuous |
|---|---|---|
| lifecycle | drain files, stop | runs forever |
| use | reproducible backfill / batch-as-stream | live dashboards/pipelines |

## Is Structured Streaming the right tool? (vs Kafka Connect vs Flink)
First, untangle the names: **Spark Streaming (DStreams)** is the *legacy* API — don't use it.
**Structured Streaming** is the current one (this chapter). And **Kafka** is a *transport*,
not a processor — you don't pick "Spark **or** Kafka", you usually do **Kafka → Spark**
(`readStream.format("kafka")`). The real choice is what reads/processes the stream:

- **Just landing raw events (no transform)** → a **Kafka Connect sink** (S3/GCS), **Kinesis
  Firehose**, or **Pub/Sub → GCS**. Low-code, cheap, exactly-once handled for you. Don't reach
  for Spark, and **never hand-roll a `KafkaConsumer` loop** (you'd reinvent offsets, batching,
  exactly-once — all easy to get subtly wrong).
- **Transform on ingest** (dedup, joins, windows, schema enforcement, exactly-once into a
  lakehouse) → **Spark Structured Streaming**. Its superpower: **the same DataFrame code for
  batch and stream**, plus exactly-once when paired with a Delta/Iceberg sink (offsets + table
  commit advance atomically). Trade-off: **micro-batch latency (~seconds)**, fine for analytics.
- **Sub-second, per-event, complex event-time / CEP** → **Apache Flink** (true streaming).

So Structured Streaming is a mainstream, viable choice — it competes with **Flink** (for
real-time) and **simple sink connectors** (for plain landing), not with Kafka, which is its
*source*. Our file-source demo stands in for that Kafka source; swap `readStream.json(dir)`
for `readStream.format("kafka")` and the rest barely changes.
