"""
generate_clickstream.py — Phase 11: emit clickstream events as JSON files.

Structured Streaming (file source) watches a FOLDER and treats each new file as
new data. So this writer drops one-or-more JSON-Lines files into that folder,
with unique names, so every run ADDS files (simulating events arriving over time).

Usage:
  python jobs/generate_clickstream.py                       # 1 file, 200 events
  python jobs/generate_clickstream.py --files 3 --events 500 # 3 files x 500 events
"""

import argparse, json, os, random, time
from datetime import datetime, timedelta

# skewed tenant distribution (inlined so this phase folder is self-contained)
TENANTS = [
    ("tenant_mega_001", 0.60), ("tenant_mid_001", 0.10), ("tenant_mid_002", 0.10),
    ("tenant_small_001", 0.02), ("tenant_small_002", 0.02), ("tenant_small_003", 0.02),
    ("tenant_small_004", 0.02), ("tenant_small_005", 0.02), ("tenant_small_006", 0.05),
    ("tenant_small_007", 0.05),
]


def weighted_tenant():
    r, cum = random.random(), 0.0
    for tenant_id, w in TENANTS:
        cum += w
        if r <= cum:
            return tenant_id
    return TENANTS[-1][0]


STREAM_DIR = "data/landing/clickstream_stream"
EVENT_TYPES = ["view", "view", "view", "click", "add_to_cart", "search", "purchase"]
DEVICES = ["mobile", "mobile", "desktop", "tablet"]

# a FIXED base time so 1-minute windows are reproducible; events spread over 10 min
BASE_TS = datetime(2026, 7, 4, 12, 0, 0)


def make_event():
    t = weighted_tenant()
    ts = BASE_TS + timedelta(seconds=random.randint(0, 599))   # within a 10-min span
    return {
        "tenant_id": t,
        "event_id": f"e_{random.getrandbits(64):016x}",
        "session_id": f"s_{random.getrandbits(48):012x}",
        "customer_id": f"c_{random.getrandbits(48):012x}",
        "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "event_date": ts.strftime("%Y-%m-%d"),
        "event_type": random.choice(EVENT_TYPES),
        "product_id": f"p_{t[-3:]}_{random.randint(1, 200):04d}",
        "page_url": f"/product/{random.randint(1, 200)}",
        "device_type": random.choice(DEVICES),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=1)
    ap.add_argument("--events", type=int, default=200)
    args = ap.parse_args()

    os.makedirs(STREAM_DIR, exist_ok=True)
    for i in range(args.files):
        # unique name (timestamp + random) so each file is "new" to the stream
        fname = f"events_{int(time.time()*1000)}_{random.getrandbits(16):04x}.json"
        path = os.path.join(STREAM_DIR, fname)
        with open(path, "w") as f:
            for _ in range(args.events):
                f.write(json.dumps(make_event()) + "\n")   # JSON Lines: one object per line
        print(f"  wrote {args.events} events -> {path}")
        time.sleep(0.01)  # ensure distinct timestamps in filenames

    total = len([x for x in os.listdir(STREAM_DIR) if x.endswith(".json")])
    print(f"stream folder now holds {total} json file(s)")


if __name__ == "__main__":
    main()
