"""generate_data.py - seeded synthetic e-commerce workloads with injected defects.

Writes orders, clickstream and payment-page CSV files for one volume tier and an
injection log (ground truth) recording every deliberately defective record.

    python generate_data.py --rows 10000 --out tier_10k --seed 42
    python generate_data.py --rows 10000 --out tier_10k_utc --seed 42 --utc

With --utc, every order and clickstream timestamp is written as ISO 8601 in UTC with a 'Z' offset
instead of local Europe/Berlin time. The random sequence is unchanged, so the records, the injected
defects and the injection log are identical to the default run; only the timestamp strings differ.
"""
import argparse
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from faker import Faker

BERLIN = ZoneInfo("Europe/Berlin")
START = datetime(2025, 1, 1, tzinfo=timezone.utc)
YEAR_S = 365 * 24 * 3600
DEFECT_RATE = 0.02                     # share of records per dataset that receive one defect
PAGE_ROWS = 10_000                     # payment export page size

H_ORD = ["order_id", "line_number", "order_ts", "customer_id", "customer_email", "customer_name",
         "ship_country", "ship_postcode", "product_sku", "product_name", "category", "quantity",
         "unit_price", "discount_amount", "shipping_amount", "total_amount", "currency_code"]
H_CLK = ["event_id", "session_id", "event_ts", "customer_id", "event_payload"]
H_PAY = ["payment_id", "created", "gateway", "transaction_type", "status", "payment_method",
         "amount_minor", "currency", "payer_email", "payer_name", "metadata"]
CATEGORIES = ["Kitchen", "Home", "Garden", "Toys", "Fashion", "Electronics", "Beauty", "Sports"]
CURRENCIES = ["EUR"] * 8 + ["USD", "GBP", "CHF", "PLN"]


def utc_instant(rng):
    return START + timedelta(seconds=rng.randrange(YEAR_S))


def pick_defects(rng, n, kinds):
    """Choose distinct record indices (never the first 10) and assign defect kinds round-robin."""
    count = int(n * DEFECT_RATE)
    idx = rng.sample(range(10, n), count)
    return {i: kinds[k % len(kinds)] for k, i in enumerate(sorted(idx))}


def build_pools(fake, rng, n):
    customers = []
    for c in range(max(1_000, n // 10)):
        country = rng.choice(["DE"] * 6 + ["AT", "CH", "PL", "FR"])
        customers.append({"id": f"C{c:07d}", "email": fake.unique.email(), "name": fake.name(),
                          "country": country, "postcode": fake.postcode()})
    products = [{"sku": f"SKU-{p:05d}", "name": f"Product {p:05d}", "cat": rng.choice(CATEGORIES),
                 "price": round(rng.uniform(3, 250), 2)} for p in range(2_000)]
    # 5 % of products are reclassified and 5 % of customers relocate at a random point in the year
    reclass = {p["sku"]: (utc_instant(rng), rng.choice(CATEGORIES)) for p in rng.sample(products, 100)}
    moves = {c["id"]: (utc_instant(rng), rng.choice(["DE", "AT", "CH"]), fake.postcode())
             for c in rng.sample(customers, len(customers) // 20)}
    return customers, products, reclass, moves


def orders(n, rng, customers, products, reclass, moves, log, utc=False):
    rows, line_no, order_no = [], 0, 0
    while len(rows) < n:
        order_no += 1
        cust, ts = rng.choice(customers), utc_instant(rng)
        for line in range(1, rng.randint(1, 3) + 1):
            if len(rows) == n:
                break
            p = rng.choice(products)
            cat = reclass[p["sku"]][1] if p["sku"] in reclass and ts >= reclass[p["sku"]][0] else p["cat"]
            country, postcode = cust["country"], cust["postcode"]
            if cust["id"] in moves and ts >= moves[cust["id"]][0]:
                country, postcode = moves[cust["id"]][1], moves[cust["id"]][2]
            qty, disc, ship = rng.randint(1, 4), rng.choice([0, 0, 0, 2.5, 5]), rng.choice([0, 4.9, 6.9])
            total = round(p["price"] * qty + ship - disc, 2)
            local = (ts.strftime("%Y-%m-%dT%H:%M:%SZ") if utc                 # supplementary UTC run
                     else ts.astimezone(BERLIN).strftime("%Y-%m-%d %H:%M:%S"))   # storefront exports local time
            rows.append([f"O{order_no:08d}", line, local, cust["id"], cust["email"], cust["name"], country,
                         postcode, p["sku"], p["name"], cat, qty, p["price"], disc, ship, total,
                         rng.choice(CURRENCIES).lower() if rng.random() < 0.05 else rng.choice(CURRENCIES)])
    defects = pick_defects(rng, n, ["NULL_VIOLATION", "TYPE_ERROR", "ARITHMETIC_MISMATCH",
                                    "DOMAIN_VIOLATION", "DUPLICATE_KEY"])
    for i, code in defects.items():
        r = rows[i]
        if code == "NULL_VIOLATION":
            r[11] = ""
        elif code == "TYPE_ERROR":
            r[11] = "two"
        elif code == "ARITHMETIC_MISMATCH":
            r[15] = round(float(r[15]) + 10, 2)
        elif code == "DOMAIN_VIOLATION":
            r[16] = "XYZ"
        elif code == "DUPLICATE_KEY":                   # repeat the key of an earlier clean row
            src = next(j for j in range(i - 1, -1, -1) if j not in defects)
            rows[i] = list(rows[src])
        log.append(("orders", "orders_001.csv", i + 1, code))
    return rows


def clickstream(n, rng, customers, products, log, utc=False):
    rows = []
    for i in range(n):
        ts = utc_instant(rng)
        stamp = ts.strftime("%Y-%m-%dT%H:%M:%SZ") if (i % 2 or utc) else ts.astimezone(BERLIN).strftime("%Y-%m-%dT%H:%M:%S")
        etype = rng.choice(["page_view"] * 6 + ["add_to_cart", "remove_from_cart"])
        payload = {"type": etype, "page": f"/p/{rng.randrange(5000)}"}
        if etype != "page_view":
            payload.update(sku=rng.choice(products)["sku"], qty=rng.randint(1, 3))
        cust = "" if rng.random() < 0.3 else rng.choice(customers)["id"]
        rows.append([f"E{i:09d}", f"S{i // 8:08d}", stamp, cust, json.dumps(payload)])
    defects = pick_defects(rng, n, ["JSON_PARSE_ERROR", "NULL_VIOLATION", "DOMAIN_VIOLATION",
                                    "TYPE_ERROR", "DUPLICATE_KEY"])
    for i, code in defects.items():
        r = rows[i]
        if code == "JSON_PARSE_ERROR":
            r[4] = r[4][:-5]
        elif code == "NULL_VIOLATION":
            r[4] = json.dumps({"page": "/x"})
        elif code == "DOMAIN_VIOLATION":
            r[4] = json.dumps({"type": "scroll", "page": "/x"})
        elif code == "TYPE_ERROR":
            r[2] = "2025-13-45T99:00:00"
        elif code == "DUPLICATE_KEY":
            src = next(j for j in range(i - 1, -1, -1) if j not in defects)
            r[0] = rows[src][0]
        log.append(("clickstream", "clickstream_001.csv", i + 1, code))
    return rows


def payments(n, rng, fake, log):
    base = []
    for i in range(n):
        refund = rng.random() < 0.05
        amount = rng.randrange(300, 50_000)
        base.append([f"py_{i:09d}", int(utc_instant(rng).timestamp()), rng.choice(["stripe", "paypal"]),
                     "refund" if refund else "charge", rng.choice(["succeeded"] * 8 + ["pending", "failed"]),
                     rng.choice(["card", "sepa_debit", "paypal"]), -amount if refund else amount,
                     rng.choice(CURRENCIES).lower(), fake.email(), fake.name(),
                     json.dumps({"order_id": f"O{rng.randrange(1, n):08d}", "channel": rng.choice(["web", "app"])})])
    defects = pick_defects(rng, n, ["NULL_VIOLATION", "INVALID_CURRENCY", "NEGATIVE_AMOUNT", "TYPE_ERROR"])
    for i, code in defects.items():
        r = base[i]
        if code == "NULL_VIOLATION":
            r[7] = ""
        elif code == "INVALID_CURRENCY":
            r[7] = "xxx"
        elif code == "NEGATIVE_AMOUNT":
            r[3], r[6] = "charge", -abs(r[6])
        elif code == "TYPE_ERROR":
            r[6] = "abc"
    pages, start = [], 0
    while start < n:                                   # each page repeats the last row of the previous one
        lo = max(0, start - 1)
        pages.append(base[lo:start + PAGE_ROWS])
        start += PAGE_ROWS
    for p, page in enumerate(pages, 1):
        name = f"payments_page_{p:04d}.csv"
        offset = 0 if p == 1 else 1
        for rec, row in enumerate(page, 1):
            i = (p - 1) * PAGE_ROWS - offset + rec - 1
            if p > 1 and rec == 1:
                if (i) not in defects:                 # overlap row: duplicate of an already loaded payment
                    log.append(("payments", name, rec, "DUPLICATE_KEY"))
                else:
                    log.append(("payments", name, rec, defects[i]))
            elif i in defects:
                log.append(("payments", name, rec, defects[i]))
    return pages


def write(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--utc", action="store_true", help="write all order and clickstream timestamps in UTC")
    a = ap.parse_args()
    rng, fake = random.Random(a.seed), Faker("de_DE")
    fake.seed_instance(a.seed)
    a.out.mkdir(parents=True, exist_ok=True)
    customers, products, reclass, moves = build_pools(fake, rng, a.rows)
    log = []
    write(a.out / "orders_001.csv", H_ORD, orders(a.rows, rng, customers, products, reclass, moves, log, a.utc))
    write(a.out / "clickstream_001.csv", H_CLK, clickstream(a.rows, rng, customers, products, log, a.utc))
    for p, page in enumerate(payments(a.rows, rng, fake, log), 1):
        write(a.out / f"payments_page_{p:04d}.csv", H_PAY, page)
    write(a.out / "injection_log.csv", ["dataset", "file", "record_number", "expected_code"], log)
    print(f"{a.out}: {a.rows} rows per dataset, {len(log)} injected defects")


if __name__ == "__main__":
    main()
