#!/usr/bin/env python3
"""baseline_returns.py - hard-coded ETL for a newly onboarded returns source (RQ3, scenario S4)."""
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
from psycopg2.extras import Json, execute_values

INBOX, ARCHIVE = Path("data/inbox"), Path("data/archive")
TZ = "Europe/Berlin"
REQUIRED = ["return_id", "order_id", "line_number", "returned_at", "product_sku", "quantity",
            "refund_amount", "currency", "reason"]
CURRENCIES = {"EUR", "USD", "GBP", "CHF", "PLN", "SEK"}
REASONS = {"damaged", "wrong_item", "not_as_described", "changed_mind"}
OFFSET_RE = r"(?:Z|[+-]\d{2}:?\d{2})$"


class ContractViolation(Exception):
    pass


def read(path):
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ContractViolation(f"missing mandatory column(s) {', '.join(missing)}")
    df = df[REQUIRED].astype(object)
    df = df.where(df.apply(lambda s: s.str.strip() != ""), None)
    df.index = pd.RangeIndex(1, len(df) + 1)
    return df


def parse_ts(s):
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns, UTC]")
    present = s.notna()
    vals = s[present].astype(str).str.strip()
    aware = vals.str.contains(OFFSET_RE, regex=True)
    if aware.any():
        out[aware[aware].index] = pd.to_datetime(vals[aware], format="ISO8601", utc=True, errors="coerce")
    naive = vals[~aware]
    if len(naive):
        loc = pd.to_datetime(naive, format="ISO8601", errors="coerce").dt.tz_localize(
            TZ, ambiguous="NaT", nonexistent="NaT")
        out[naive.index] = loc.dt.tz_convert("UTC")
    return out


def validate(df):
    code = pd.Series(None, index=df.index, dtype=object)
    msg = pd.Series(None, index=df.index, dtype=object)

    def flag(mask, c, m):
        sel = mask.fillna(False).astype(bool) & code.isna()
        code[sel] = c
        msg[sel] = m

    out = pd.DataFrame(index=df.index)
    df["currency"] = df["currency"].map(lambda s: None if s is None else s.strip().upper())
    for c in REQUIRED:
        flag(df[c].isna(), "NULL_VIOLATION", f"{c}: required value missing")
    for c, lo, hi in [("line_number", 1, 32767), ("quantity", 1, None)]:
        out[c] = pd.to_numeric(df[c], errors="coerce")
        flag(df[c].notna() & (out[c].isna() | (out[c] % 1 != 0)), "TYPE_ERROR", f"{c}: not a valid integer")
        flag(out[c] < lo, "DOMAIN_VIOLATION", f"{c}: below minimum {lo}")
        if hi:
            flag(out[c] > hi, "DOMAIN_VIOLATION", f"{c}: above maximum {hi}")
    out["refund_amount"] = pd.to_numeric(df["refund_amount"], errors="coerce")
    flag(df["refund_amount"].notna() & out["refund_amount"].isna(), "TYPE_ERROR", "refund_amount: not a valid decimal")
    flag(out["refund_amount"] < 0, "DOMAIN_VIOLATION", "refund_amount: below minimum 0")
    out["returned_at_utc"] = parse_ts(df["returned_at"])
    flag(df["returned_at"].notna() & out["returned_at_utc"].isna(), "TYPE_ERROR", "returned_at: not a valid timestamp")
    flag(df["currency"].notna() & ~df["currency"].isin(CURRENCIES), "INVALID_CURRENCY", "currency: not an allowed value")
    flag(df["reason"].notna() & ~df["reason"].isin(REASONS), "DOMAIN_VIOLATION", "reason: not an allowed value")
    dup = df.loc[code.isna(), ["return_id"]].duplicated(keep="first").reindex(df.index, fill_value=False)
    flag(dup, "DUPLICATE_KEY", "duplicate business key within file (first occurrence kept)")
    for c in ["return_id", "order_id", "product_sku", "reason"]:
        out[c] = df[c]
    out["currency_code"] = df["currency"]
    return out, code, msg


def py(v):
    if v is None or pd.isna(v):
        return None
    return v.to_pydatetime() if isinstance(v, pd.Timestamp) else v


def load(cur, typed, run_id):
    cur.execute("CREATE TEMP TABLE stg (_rn INTEGER, return_id TEXT, order_id TEXT, line_number BIGINT, "
                "returned_at_utc TIMESTAMPTZ, product_sku TEXT, quantity BIGINT, refund_amount NUMERIC, "
                "currency_code TEXT, reason TEXT) ON COMMIT DROP")
    rows = [(int(rn), r.return_id, r.order_id, int(r.line_number), py(r.returned_at_utc), r.product_sku,
             int(r.quantity), Decimal(str(r.refund_amount)), r.currency_code, r.reason)
            for rn, r in typed.iterrows()]
    execute_values(cur, "INSERT INTO stg VALUES %s", rows, page_size=5000)
    cur.execute("""
        INSERT INTO dwh.fact_return (return_id, order_id, line_number, date_key, product_sk, returned_at_utc,
               quantity, refund_amount, currency_code, reason, run_id)
        SELECT s.return_id, s.order_id, s.line_number, dd.date_key, p.product_sk, s.returned_at_utc, s.quantity,
               s.refund_amount, s.currency_code, s.reason, %s::UUID
        FROM stg s
        JOIN dwh.dim_date dd ON dd.date_key = to_char(s.returned_at_utc AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER
        LEFT JOIN dwh.dim_product p ON p.product_sku = s.product_sku
             AND s.returned_at_utc >= p.valid_from AND s.returned_at_utc < p.valid_to
        ON CONFLICT DO NOTHING RETURNING return_id""", (run_id,))
    inserted = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT s._rn, s.return_id, EXISTS (SELECT 1 FROM dwh.fact_return f "
                "WHERE f.return_id = s.return_id) FROM stg s")
    rejected = {}
    for rn, rid, exists in cur.fetchall():
        if rid not in inserted:
            rejected[rn] = ("DUPLICATE_KEY", "business key already present in the warehouse") if exists else \
                ("DOMAIN_VIOLATION", "date outside dim_date")
    return len(inserted), rejected


def process(conn, path, run_id):
    t0, started = time.perf_counter(), datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO etl.pipeline_logs (run_id, source_name, source_file, started_at, status) "
                    "VALUES (%s, 'returns', %s, %s, 'RUNNING') RETURNING log_id", (run_id, path.name, started))
        log_id = cur.fetchone()[0]
    conn.commit()
    try:
        raw = read(path)
        typed, code, msg = validate(raw.copy())
        valid = code.isna()
        typed = typed[valid]
        quarantine = [(rn, code[rn], msg[rn]) for rn in raw.index[~valid]]
        with conn.cursor() as cur:
            inserted, rejected = load(cur, typed, run_id) if not typed.empty else (0, {})
            quarantine += [(rn, c, m) for rn, (c, m) in rejected.items()]
            recs = raw.astype(object).where(raw.notna(), None)
            execute_values(cur, "INSERT INTO etl.quarantine_staging (run_id, log_id, source_name, source_file, "
                                "source_row_number, error_code, error_message, raw_record) VALUES %s",
                           [(run_id, log_id, "returns", path.name, int(rn), c, m, Json(recs.loc[rn].to_dict()))
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
    for path in sorted(INBOX.glob("returns_*.csv")):
        try:
            process(conn, path, run_id)
        except ContractViolation as exc:
            print(f"halted: {exc}", file=sys.stderr)
            sys.exit(2)
    conn.close()


if __name__ == "__main__":
    main()
