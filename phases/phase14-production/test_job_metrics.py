"""
test_job_metrics.py — Phase 14: turn emitted metrics into a REGRESSION GUARD.

Metrics you only look at are useless. This reads the LATEST record from
metrics.jsonl and asserts it sits within expected bands. Wire it into CI (or run
it right after the job) and a bad run -- empty mart, row-count collapse, file
explosion, runtime spike -- fails the build instead of poisoning dashboards.

No Spark needed: it's just reading the JSONL log.
"""

import os, json
import pytest

LOG = "data/gold/_job_metrics/metrics.jsonl"

# expected operating bands for gold_daily_revenue at scale=10
BANDS = {
    "output_rows":  (8_000, 12_000),
    "input_items":  (2_400_000, 2_600_000),
    "runtime_s":    (0, 120),
    "output_files": (0, 300),
}


def latest_metric():
    if not os.path.exists(LOG):
        pytest.skip("no metrics log yet — run metrics_harness.py first")
    with open(LOG) as f:
        lines = [l for l in f if l.strip()]
    return json.loads(lines[-1])          # the most recent run


@pytest.mark.parametrize("field,bounds", [(k, v) for k, v in BANDS.items()])
def test_metric_in_band(field, bounds):
    lo, hi = bounds
    val = latest_metric().get(field)
    assert val is not None, f"metric '{field}' missing from latest run"
    assert lo <= val <= hi, f"{field}={val} outside expected band [{lo}, {hi}]"


def test_mart_not_empty():
    """the Phase 5 bug that produced an EMPTY mart would fail HERE."""
    assert latest_metric()["output_rows"] > 0
