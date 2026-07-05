# Phase 14 — Production hardening

**Reference:** [`docs/runbook.md`](../docs/runbook.md), [`docs/tuning_notes.md`](../docs/tuning_notes.md)

## Goal
Think like a staff engineer running this for a real company: orchestration, retries,
observability, SLAs, cost, failure recovery, ownership, data contracts.

## Runbooks — the on-call answer sheet
Every production job needs a runbook (see [`docs/runbook.md`](../docs/runbook.md))
answering:
- What does this job do? Inputs? Outputs? How often does it run? Expected runtime?
- What are the common failures and their fixes?
- How do I backfill? How do I validate the output? Who owns the source? Who consumes it?

## Job metrics — emit these every run
`input rows · output rows · rejected rows · runtime seconds · shuffle read/write bytes ·
number of output files`. These make regressions **visible before a human notices broken
dashboards** — a sudden row-count drop, a file-count explosion, a runtime spike.

## The operational questions you can now answer
| question | how |
|---|---|
| How do I know the job succeeded? | `_SUCCESS` marker + metrics within bands |
| How do I know the output is correct? | the Phase 13 data-quality tests |
| How do I rerun yesterday? | incremental job, one-day window (Phase 10) |
| How do I backfill a month? | incremental job, 30-day window, dynamic overwrite |
| How do I debug a slow run? | Spark UI: heaviest stage → Max-vs-Median → skew/spill (Phase 8/9) |
| How do I prevent silent corruption? | composite keys, reconciliation tests, quarantine tables |

## Going deeper — emit & guard metrics (hands-on)
[`metrics_harness.py`](../phases/phase14-production/metrics_harness.py) wraps the mart build
and appends a metrics record (runtime, input/output/rejected rows, output files/bytes, and
**shuffle bytes pulled from Spark's REST API** at `{uiWebUrl}/api/v1/applications/{app}/stages`)
to a JSONL log. [`test_job_metrics.py`](../phases/phase14-production/test_job_metrics.py) then
asserts each metric stays in a band — a **regression guard**. We watched it catch a simulated
mart collapse (`output_rows=12`) that a naive "not empty" (`> 0`) check *passed*: **bands beat
binary checks**, and metrics tests cost ~nothing (no Spark). In production these fan out to a
monitoring backend (alerting), a warehouse table (history), and a data-observability tool
(anomaly detection) — see the note at the end of this chapter.

## The recurring themes across all phases
- **Idempotency** — a job you can safely re-run (dynamic overwrite, deterministic ids).
- **Observability** — plans, the Spark UI, and emitted metrics.
- **Data contracts** — explicit schemas + validation + tests.
- **Ownership boundaries** — who produces the source, who consumes the output.
