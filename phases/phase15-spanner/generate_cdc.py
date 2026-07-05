"""
generate_cdc.py — Phase 15: simulate a Spanner change stream for orders.

Instead of a bulk CSV export, real OLTP systems emit CHANGE events: each row
mutation becomes an INSERT / UPDATE / DELETE record with a commit timestamp.
This writes such a change log as JSON files (data/landing/orders_cdc/).

Narrative (so the result is verifiable):
  t0  INSERT every order        (status=PENDING)
  t1  UPDATE 60% -> COMPLETE
  t2  UPDATE 10% again          (total corrected)
  t3  DELETE 8%                 (cancelled -> tombstone)
Events are SHUFFLED across files to simulate out-of-order arrival: the
commit_ts (not file/arrival order) is the source of truth for "latest".
"""

import argparse, json, os, random
from datetime import datetime, timedelta

OUT = "data/landing/orders_cdc"
TENANTS = ["tenant_mega_001", "tenant_mid_001", "tenant_small_003"]
SEED = 7


def evt(op, ts, seq, tenant, oid, status=None, total=None):
    return {"op": op, "commit_ts": ts.strftime("%Y-%m-%d %H:%M:%S"), "seq": seq,
            "tenant_id": tenant, "order_id": oid, "status": status, "total_amount": total}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=1000)
    args = ap.parse_args()
    random.seed(SEED)

    n = args.orders
    base = datetime(2026, 7, 5, 9, 0, 0)
    seq = 0
    events = []
    orders = [(random.choice(TENANTS), f"o_{random.getrandbits(64):016x}") for _ in range(n)]

    # t0: INSERT all (PENDING)
    for (t, oid) in orders:
        events.append(evt("I", base, seq, t, oid, "PENDING", round(random.uniform(20, 500), 2))); seq += 1
    # t1: 60% UPDATE -> COMPLETE
    for (t, oid) in random.sample(orders, int(n * 0.60)):
        events.append(evt("U", base + timedelta(minutes=5), seq, t, oid, "COMPLETE",
                          round(random.uniform(20, 500), 2))); seq += 1
    # t2: 10% UPDATE again (correction)
    for (t, oid) in random.sample(orders, int(n * 0.10)):
        events.append(evt("U", base + timedelta(minutes=10), seq, t, oid, "COMPLETE",
                          round(random.uniform(20, 500), 2))); seq += 1
    # t3: 8% DELETE (tombstone)
    deleted = random.sample(orders, int(n * 0.08))
    for (t, oid) in deleted:
        events.append(evt("D", base + timedelta(minutes=15), seq, t, oid)); seq += 1

    # shuffle to simulate OUT-OF-ORDER arrival, then write in chunks
    random.shuffle(events)
    os.makedirs(OUT, exist_ok=True)
    for i in range(0, len(events), 500):
        with open(os.path.join(OUT, f"cdc_{i:06d}.json"), "w") as f:
            for e in events[i:i + 500]:
                f.write(json.dumps(e) + "\n")

    n_del = len(set(deleted))
    print(f"orders (distinct)     : {n:,}")
    print(f"change events written : {len(events):,}  (I+U+U+D)")
    print(f"deleted (tombstoned)  : {n_del:,}")
    print(f"=> EXPECTED current-state rows after applying the log: {n - n_del:,}")


if __name__ == "__main__":
    main()
