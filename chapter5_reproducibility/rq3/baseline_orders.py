#!/usr/bin/env python3
"""baseline_orders.py - hard-coded ETL for the orders source (RQ3 baseline).

Functionally equivalent to etl_engine.py for orders_*.csv, but written in the
conventional style: column names, types, rules and privacy handling are coded
directly. Written cleanly (named constants, helper functions) so that the
comparison is against a reasonable hard-coded script, not a strawman.
"""
import hashlib
import hmac
import json
import os
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_batch, execute_values

INBOX, ARCHIVE = Path("data/inbox"), Path("data/archive")
TZ = "Europe/Berlin"
REQUIRED = ["order_id", "line_number", "order_ts", "customer_id", "product_sku", "product_name",
            "category", "quantity", "unit_price", "total_amount", "currency_code"]
ALL_COLUMNS = REQUIRED + ["customer_email", "customer_name", "ship_country", "ship_postcode",
                          "discount_amount", "shipping_amount"]
CURRENCIES = {"EUR", "USD", "GBP", "CHF", "PLN", "SEK"}
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
KEY = os.environ["ETL_PII_SALT"].encode()


class ContractViolation(Exception):
    pass


def h(value, lower=False):
    if value is None:
        return None
    text = str(value).strip()
    return hmac.new(KEY, (text.lower() if lower else text).encode(), hashlib.sha256).hexdigest()


def read(path):
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ContractViolation(f"missing mandatory column(s) {', '.join(missing)}")
    for c in ALL_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[ALL_COLUMNS].astype(object)
    df = df.where(df.apply(lambda s: s.str.strip() != ""), None)
    df.index = pd.RangeIndex(1, len(df) + 1)
    return df


def validate(df):
    code = pd.Series(None, index=df.index, dtype=object)
    msg = pd.Series(None, index=df.index, dtype=object)

    def flag(mask, c, m):
        sel = mask.fillna(False).astype(bool) & code.isna()
        code[sel] = c
        msg[sel] = m

    out = pd.DataFrame(index=df.index)
    df["currency_code"] = df["currency_code"].map(lambda s: None if s is None else s.strip().upper())
    for c in REQUIRED:
        flag(df[c].isna(), "NULL_VIOLATION", f"{c}: required value missing")
    for c in ["discount_amount", "shipping_amount"]:
        df[c] = df[c].where(df[c].notna(), "0")

    out["line_number"] = pd.to_numeric(df["line_number"], errors="coerce")
    flag(df["line_number"].notna() & (out["line_number"].isna() | (out["line_number"] % 1 != 0)),
         "TYPE_ERROR", "line_number: not a valid integer")
    flag(out["line_number"] < 1, "DOMAIN_VIOLATION", "line_number: below minimum 1")
    out["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    flag(df["quantity"].notna() & (out["quantity"].isna() | (out["quantity"] % 1 != 0)),
         "TYPE_ERROR", "quantity: not a valid integer")
    flag(out["quantity"] < 1, "DOMAIN_VIOLATION", "quantity: below minimum 1")
    for c in ["unit_price", "discount_amount", "shipping_amount", "total_amount"]:
        out[c] = pd.to_numeric(df[c], errors="coerce")
        flag(df[c].notna() & out[c].isna(), "TYPE_ERROR", f"{c}: not a valid decimal")
    for c in ["unit_price", "discount_amount", "shipping_amount"]:
        flag(out[c] < 0, "DOMAIN_VIOLATION", f"{c}: below minimum 0")

    naive = pd.to_datetime(df["order_ts"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    out["order_ts_utc"] = naive.dt.tz_localize(TZ, ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
    flag(df["order_ts"].notna() & out["order_ts_utc"].isna(), "TYPE_ERROR", "order_ts: not a valid timestamp")

    flag(df["customer_email"].notna() & ~df["customer_email"].astype(str).str.fullmatch(EMAIL_RE),
         "DOMAIN_VIOLATION", "customer_email: does not match the required pattern (value withheld)")
    flag(df["ship_country"].notna() & ~df["ship_country"].astype(str).str.fullmatch(r"^[A-Z]{2}$"),
         "DOMAIN_VIOLATION", "ship_country: does not match the required pattern")
    flag(df["currency_code"].notna() & ~df["currency_code"].isin(CURRENCIES),
         "DOMAIN_VIOLATION", "currency_code: not an allowed value")

    ok = code.isna()
    diff = (out["total_amount"] - (out["unit_price"] * out["quantity"] + out["shipping_amount"]
                                   - out["discount_amount"])).abs()
    flag(ok & ~(diff <= 0.01), "ARITHMETIC_MISMATCH", "rule 'total_arithmetic' violated")
    dup = df.loc[code.isna(), ["order_id", "line_number"]].duplicated(keep="first").reindex(df.index, fill_value=False)
    flag(dup, "DUPLICATE_KEY", "duplicate business key within file (first occurrence kept)")

    for c in ["order_id", "product_sku", "product_name", "category", "currency_code", "ship_country"]:
        out[c] = df[c]
    return out, code, msg


def pseudonymise(df):
    df = df.copy()
    df["customer_hash"] = df["customer_id"].map(h)
    df["email_hash"] = df["customer_email"].map(lambda v: h(v, lower=True))
    df["ship_postcode_prefix"] = df["ship_postcode"].map(lambda v: None if v is None else str(v).strip().upper()[:2])
    return df.drop(columns=["customer_id", "customer_email", "customer_name", "ship_postcode"])


def scd2(cur, rows, table, sk, bk, attrs):
    rows = rows.sort_values("order_ts_utc", kind="stable")
    probe = rows[attrs].fillna("\x00")
    rows = rows[probe.ne(probe.groupby(rows[bk]).shift()).any(axis=1)]
    cur.execute(f"SELECT {sk}, {bk}, {', '.join(attrs)}, valid_from FROM {table} "
                f"WHERE is_current AND {bk} = ANY(%s)", (rows[bk].unique().tolist(),))
    current = {r[1]: (r[0], tuple(r[2:-1]), r[-1]) for r in cur.fetchall()}
    closes, inserts = [], []
    for key, grp in rows.groupby(bk, sort=False):
        chain = []
        for rec in grp.itertuples(index=False):
            a = tuple(None if pd.isna(getattr(rec, x)) else getattr(rec, x) for x in attrs)
            eff = rec.order_ts_utc.to_pydatetime()
            if chain and chain[-1][1] == eff:
                chain[-1][0] = a
            elif not chain or chain[-1][0] != a:
                chain.append([a, eff])
        if key in current:
            cur_sk, cur_attrs, cur_from = current[key]
            if cur_from.tzinfo is not None:
                chain = [c for c in chain if c[1] > cur_from]
            while chain and chain[0][0] == cur_attrs:
                chain.pop(0)
            if not chain:
                continue
            closes.append((chain[0][1], cur_sk))
        else:
            chain[0][1] = "-infinity"
        for i, (a, eff) in enumerate(chain):
            last = i == len(chain) - 1
            inserts.append((key, *a, eff, "infinity" if last else chain[i + 1][1], last))
    if closes:
        execute_batch(cur, f"UPDATE {table} SET valid_to = %s, is_current = FALSE WHERE {sk} = %s", closes)
    if inserts:
        execute_values(cur, f"INSERT INTO {table} ({bk}, {', '.join(attrs)}, valid_from, valid_to, is_current) "
                            f"VALUES %s", inserts, page_size=5000)


def py(v):
    if v is None or (not isinstance(v, (dict, list)) and pd.isna(v)):
        return None
    return v.to_pydatetime() if isinstance(v, pd.Timestamp) else v


def load(cur, typed, run_id):
    scd2(cur, typed, "dwh.dim_customer", "customer_sk", "customer_hash", ["ship_country", "ship_postcode_prefix"])
    scd2(cur, typed, "dwh.dim_product", "product_sk", "product_sku", ["product_name", "category"])
    emails = typed[["customer_hash", "email_hash"]].dropna().groupby("customer_hash")["email_hash"].last()
    if not emails.empty:
        execute_values(cur, "UPDATE dwh.dim_customer d SET email_hash = v.val FROM (VALUES %s) AS v(key, val) "
                            "WHERE d.customer_hash = v.key AND d.email_hash IS DISTINCT FROM v.val",
                       list(emails.items()))
    cur.execute("CREATE TEMP TABLE stg (_rn INTEGER, order_id TEXT, line_number BIGINT, order_ts_utc TIMESTAMPTZ, "
                "customer_hash TEXT, product_sku TEXT, quantity BIGINT, unit_price NUMERIC, discount_amount NUMERIC, "
                "shipping_amount NUMERIC, total_amount NUMERIC, currency_code TEXT) ON COMMIT DROP")
    rows = [(int(rn), r.order_id, int(r.line_number), py(r.order_ts_utc), r.customer_hash, r.product_sku,
             int(r.quantity), Decimal(str(r.unit_price)), Decimal(str(r.discount_amount)),
             Decimal(str(r.shipping_amount)), Decimal(str(r.total_amount)), r.currency_code)
            for rn, r in typed.iterrows()]
    execute_values(cur, "INSERT INTO stg VALUES %s", rows, page_size=5000)
    cur.execute("""
        INSERT INTO dwh.fact_order_line (order_id, line_number, date_key, customer_sk, product_sk, order_ts_utc,
               quantity, unit_price, discount_amount, shipping_amount, total_amount, currency_code, run_id)
        SELECT s.order_id, s.line_number, dd.date_key, c.customer_sk, p.product_sk, s.order_ts_utc, s.quantity,
               s.unit_price, s.discount_amount, s.shipping_amount, s.total_amount, s.currency_code, %s::UUID
        FROM stg s
        JOIN dwh.dim_date dd ON dd.date_key = to_char(s.order_ts_utc AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER
        JOIN dwh.dim_customer c ON c.customer_hash = s.customer_hash
             AND s.order_ts_utc >= c.valid_from AND s.order_ts_utc < c.valid_to
        JOIN dwh.dim_product p ON p.product_sku = s.product_sku
             AND s.order_ts_utc >= p.valid_from AND s.order_ts_utc < p.valid_to
        ON CONFLICT DO NOTHING RETURNING order_id, line_number""", (run_id,))
    inserted = {(r[0], str(r[1])) for r in cur.fetchall()}
    cur.execute("SELECT s._rn, s.order_id, s.line_number::TEXT, EXISTS (SELECT 1 FROM dwh.fact_order_line f "
                "WHERE f.order_id = s.order_id AND f.line_number = s.line_number) FROM stg s")
    rejected = {}
    for rn, oid, ln, exists in cur.fetchall():
        if (oid, ln) not in inserted:
            rejected[rn] = ("DUPLICATE_KEY", "business key already present in the warehouse") if exists else \
                ("DOMAIN_VIOLATION", "date outside dim_date or no dimension version valid at event time")
    return len(inserted), rejected


def process(conn, path, run_id):
    t0, started = time.perf_counter(), datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO etl.pipeline_logs (run_id, source_name, source_file, started_at, status) "
                    "VALUES (%s, 'orders', %s, %s, 'RUNNING') RETURNING log_id", (run_id, path.name, started))
        log_id = cur.fetchone()[0]
    conn.commit()
    try:
        raw = read(path)
        typed, code, msg = validate(raw.copy())
        safe = pseudonymise(raw)
        valid = code.isna()
        typed = typed.join(safe[["customer_hash", "email_hash", "ship_postcode_prefix"]])[valid]
        quarantine = [(rn, code[rn], msg[rn]) for rn in raw.index[~valid]]
        with conn.cursor() as cur:
            inserted, rejected = load(cur, typed, run_id) if not typed.empty else (0, {})
            quarantine += [(rn, c, m) for rn, (c, m) in rejected.items()]
            recs = safe.astype(object).where(safe.notna(), None)
            execute_values(cur, "INSERT INTO etl.quarantine_staging (run_id, log_id, source_name, source_file, "
                                "source_row_number, error_code, error_message, raw_record) VALUES %s",
                           [(run_id, log_id, "orders", path.name, int(rn), c, m, Json(recs.loc[rn].to_dict()))
                            for rn, c, m in quarantine], page_size=5000)
            cur.execute("UPDATE etl.pipeline_logs SET status = 'SUCCESS', finished_at = %s, execution_time = %s, "
                        "rows_read = %s, rows_inserted = %s, rows_quarantined = %s WHERE log_id = %s",
                        (datetime.now(timezone.utc), round(time.perf_counter() - t0, 3), len(raw), inserted,
                         len(quarantine), log_id))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("UPDATE etl.pipeline_logs SET status = 'FAILED', finished_at = %s, message = %s "
                        "WHERE log_id = %s", (datetime.now(timezone.utc), f"{type(exc).__name__}: {exc}", log_id))
        conn.commit()
        raise
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    shutil.move(str(path), ARCHIVE / f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{path.name}")


def main():
    conn = psycopg2.connect(os.environ["ETL_PG_DSN"])
    run_id = str(uuid.uuid4())
    for path in sorted(INBOX.glob("orders_*.csv")):
        try:
            process(conn, path, run_id)
        except ContractViolation as exc:
            print(f"halted: {exc}", file=sys.stderr)
            sys.exit(2)
    conn.close()


if __name__ == "__main__":
    main()
