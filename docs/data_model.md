# Data Model

Multi-tenant: **every table carries `tenant_id`**, and every join/dedup key is
composite `(tenant_id, <id>)` because ids are only unique *within* a tenant.

## Tenants (deliberately skewed distribution)
| tenant_id | share of rows |
|---|---|
| tenant_mega_001 | 60% |
| tenant_mid_001, tenant_mid_002 | 10% each |
| tenant_small_001..007 | remaining ~2–5% each |

The 60% mega tenant is what makes the skew labs (Phase 9) real.

## Tables
```
tenants(tenant_id, tenant_name, tier, signup_date)
customers(tenant_id, customer_id, name, email, country, signup_date)
products(tenant_id, product_id, product_name, category, base_price)
orders(tenant_id, order_id, customer_id, order_ts, order_date, status,
       currency, subtotal, tax, shipping, total_amount, payment_status)
order_items(tenant_id, order_id, order_item_id, product_id,
            quantity, unit_price, item_revenue)
clickstream(tenant_id, event_id, session_id, customer_id, event_ts, event_date,
            event_type, product_id, page_url, device_type)
```

## Keys & grain
| table | grain / dedup key |
|---|---|
| customers | (tenant_id, customer_id) |
| products | (tenant_id, product_id) |
| orders | (tenant_id, order_id) |
| order_items | (tenant_id, order_item_id) |

## Injected data-quality problems (in orders, ~2%)
`null_customer`, `null_date`, `negative_total`, exact `duplicate` rows, and
`late_arriving` records (dated far in the past — valid, but they smear the
`order_date` partitioning). Silver catches the first four; late-arriving rows are
legitimately kept.

## Reproducibility note
All ids use seeded `random.getrandbits` (NOT `uuid4`, which ignores the seed).
Regenerating with the same `--scale` yields identical data. **Always rebuild the
full pipeline after regenerating** — partial reruns mix generations and silently
break joins (Phase 5 war story).
