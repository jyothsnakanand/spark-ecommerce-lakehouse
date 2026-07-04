"""
generate_data.py — synthetic ecommerce data generator (Phase 1)

We generate raw CSV into data/landing/ to simulate data arriving from an
external transactional system (imagine a Spanner export). This is PURE PYTHON
on purpose: the landing zone is "raw external data", and generating it by hand
gives us total control over the two things this whole course is about:

    1. SKEW   — one mega-tenant owns most of the rows.
    2. DIRT   — nulls, negatives, duplicates, and late-arriving records.

Later phases (bronze/silver) will read this messy data and clean it.

Usage:
    python jobs/generate_data.py                 # small default scale
    python jobs/generate_data.py --scale 10      # 10x more rows
"""

import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta

from faker import Faker

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SEED = 42                       # deterministic: same data every run
LANDING = "data/landing"

# The heart of the course: a deliberately SKEWED tenant distribution.
# The weight is that tenant's share of ALL customers/orders/events.
# tenant_mega_001 alone owns 60% of everything -> painful skewed joins later.
TENANTS = [
    ("tenant_mega_001", 0.60),
    ("tenant_mid_001",  0.10),
    ("tenant_mid_002",  0.10),
    ("tenant_small_001", 0.02),
    ("tenant_small_002", 0.02),
    ("tenant_small_003", 0.02),
    ("tenant_small_004", 0.02),
    ("tenant_small_005", 0.02),
    ("tenant_small_006", 0.05),
    ("tenant_small_007", 0.05),
]

STATUSES = ["COMPLETE", "COMPLETE", "COMPLETE", "PENDING", "CANCELLED", "RETURNED"]
CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP"]
PAYMENT_STATUSES = ["PAID", "PAID", "PAID", "FAILED", "REFUNDED"]
CATEGORIES = ["Electronics", "Apparel", "Home", "Books",
              "Toys", "Grocery", "Beauty", "Sports"]

# A fixed catalog of product ids per tenant (kept simple for the first slice).
PRODUCTS_PER_TENANT = 200

fake = Faker()


def weighted_tenant():
    """Pick a tenant id according to the skewed weights above."""
    r = random.random()
    cumulative = 0.0
    for tenant_id, weight in TENANTS:
        cumulative += weight
        if r <= cumulative:
            return tenant_id
    return TENANTS[-1][0]


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def write_csv(path, header, rows):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {len(rows):>8,} rows -> {path}")


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------
def gen_tenants():
    header = ["tenant_id", "tenant_name", "tier", "signup_date"]
    rows = []
    for tenant_id, _ in TENANTS:
        tier = tenant_id.split("_")[1]  # mega / mid / small
        rows.append([
            tenant_id,
            fake.company(),
            tier,
            fake.date_between("-3y", "-1y").isoformat(),
        ])
    write_csv(f"{LANDING}/tenants/tenants.csv", header, rows)


def gen_products():
    """One product catalog per tenant. product_id matches what order_items use:
    f"p_{tenant_suffix}_{n:04d}", so items and products join on (tenant_id, product_id).
    Each product carries a CATEGORY -- the dimension gold_daily_revenue groups by."""
    header = ["tenant_id", "product_id", "product_name", "category", "base_price"]
    rows = []
    for tenant_id, _ in TENANTS:
        suffix = tenant_id[-3:]
        for n in range(1, PRODUCTS_PER_TENANT + 1):
            rows.append([
                tenant_id,
                f"p_{suffix}_{n:04d}",
                fake.catch_phrase(),
                random.choice(CATEGORIES),
                round(random.uniform(5, 300), 2),
            ])
    write_csv(f"{LANDING}/products/products.csv", header, rows)


def gen_customers(n):
    header = ["tenant_id", "customer_id", "name", "email", "country", "signup_date"]
    rows = []
    # Return a per-tenant list of customer ids so orders can reference real ones.
    customers_by_tenant = {t: [] for t, _ in TENANTS}
    for _ in range(n):
        tenant_id = weighted_tenant()
        # customer_id is unique WITHIN a tenant, not globally -> forces us to
        # join on (tenant_id, customer_id) later. This is the multi-tenant trap.
        # random.getrandbits IS seeded (unlike uuid4) -> reproducible ids
        cust_id = f"c_{random.getrandbits(48):012x}"
        customers_by_tenant[tenant_id].append(cust_id)
        rows.append([
            tenant_id,
            cust_id,
            fake.name(),
            fake.email(),
            fake.country_code(),
            fake.date_between("-2y", "today").isoformat(),
        ])
    write_csv(f"{LANDING}/customers/customers.csv", header, rows)
    return customers_by_tenant


def gen_orders_and_items(n_orders, customers_by_tenant, dirty_rate=0.02):
    orders_header = [
        "tenant_id", "order_id", "customer_id", "order_ts", "order_date",
        "status", "currency", "subtotal", "tax", "shipping",
        "total_amount", "payment_status",
    ]
    items_header = [
        "tenant_id", "order_id", "order_item_id", "product_id",
        "quantity", "unit_price", "item_revenue",
    ]
    orders_rows = []
    items_rows = []

    today = date.today()
    start = today - timedelta(days=90)   # 90 days of history

    for _ in range(n_orders):
        tenant_id = weighted_tenant()
        custs = customers_by_tenant[tenant_id]
        if not custs:
            continue
        customer_id = random.choice(custs)
        order_id = f"o_{random.getrandbits(64):016x}"

        # random timestamp in the last 90 days
        order_day = start + timedelta(days=random.randint(0, 89))
        order_ts = datetime(order_day.year, order_day.month, order_day.day,
                            random.randint(0, 23), random.randint(0, 59))

        # 1..4 line items
        n_items = random.randint(1, 4)
        subtotal = 0.0
        for _i in range(n_items):
            product_id = f"p_{tenant_id[-3:]}_{random.randint(1, PRODUCTS_PER_TENANT):04d}"
            qty = random.randint(1, 5)
            unit_price = round(random.uniform(5, 300), 2)
            item_rev = round(qty * unit_price, 2)
            subtotal += item_rev
            items_rows.append([
                tenant_id, order_id, f"oi_{random.getrandbits(48):012x}",
                product_id, qty, unit_price, item_rev,
            ])

        subtotal = round(subtotal, 2)
        tax = round(subtotal * 0.08, 2)
        shipping = round(random.choice([0, 0, 4.99, 9.99]), 2)
        total = round(subtotal + tax + shipping, 2)

        orders_rows.append([
            tenant_id, order_id, customer_id,
            order_ts.isoformat(), order_ts.date().isoformat(),
            random.choice(STATUSES), random.choice(CURRENCIES),
            subtotal, tax, shipping, total,
            random.choice(PAYMENT_STATUSES),
        ])

    # -----------------------------------------------------------------
    # Inject DIRT so silver has something real to clean (Phase 3).
    # -----------------------------------------------------------------
    n_dirty = int(len(orders_rows) * dirty_rate)
    dirty_log = {"null_customer": 0, "negative_total": 0, "null_date": 0,
                 "duplicate": 0, "late_arriving": 0}

    for _ in range(n_dirty):
        idx = random.randrange(len(orders_rows))
        row = orders_rows[idx]
        problem = random.choice(list(dirty_log.keys()))
        if problem == "null_customer":
            row[2] = ""                         # missing customer_id
        elif problem == "negative_total":
            row[10] = -abs(float(row[10]))      # negative revenue
        elif problem == "null_date":
            row[4] = ""                         # missing order_date
        elif problem == "duplicate":
            orders_rows.append(list(row))       # exact duplicate order row
        elif problem == "late_arriving":
            # a record whose date is way in the past (a "late" backfill)
            row[4] = (start - timedelta(days=random.randint(30, 200))).isoformat()
        dirty_log[problem] += 1

    write_csv(f"{LANDING}/orders/orders.csv", orders_header, orders_rows)
    write_csv(f"{LANDING}/order_items/order_items.csv", items_header, items_rows)
    print(f"  injected dirt: {dirty_log}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=1,
                        help="multiplier on base row counts (default 1)")
    args = parser.parse_args()

    random.seed(SEED)
    Faker.seed(SEED)

    s = args.scale
    n_customers = 10_000 * s
    n_orders = 100_000 * s

    print(f"Generating scale={s}: {n_customers:,} customers, {n_orders:,} orders")
    gen_tenants()
    customers_by_tenant = gen_customers(n_customers)
    gen_orders_and_items(n_orders, customers_by_tenant)
    gen_products()   # LAST: so it doesn't shift the RNG stream for customers/orders
    print("Done.")


if __name__ == "__main__":
    main()
