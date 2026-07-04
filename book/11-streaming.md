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
