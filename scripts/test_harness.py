#!/usr/bin/env python3
"""test_harness.py - reproducible end-to-end verification of etl_engine.py.

The harness builds an isolated PostgreSQL database from schema.sql, writes
mock order files into a temporary inbox, runs the unmodified engine as a
subprocess, and verifies the outcome directly in the database.

Scenario A - row-level isolation (orders_mock.csv, 10 records)
    rows 1-8   valid order lines
    row 9      total_amount = "CRITICAL_ERR"   -> expected TYPE_ERROR
    row 10     customer_id left blank          -> expected NULL_VIOLATION
    Expected:  exit code 0, status SUCCESS, 10 = 8 + 2, both rows in
               etl.quarantine_staging, no personal data in clear.

Scenario B - contract halt (orders_contract_violation.csv)
    The mandatory column customer_id is absent from the header.
    Expected:  exit code 2, status FAILED with CONTRACT_VIOLATION,
               0 rows inserted (the file's transaction is rolled back),
               no new fact rows, the file left in the inbox for a retry.

A blank customer_id value (scenario A) and a missing customer_id column
(scenario B) are deliberately different defects: the first is isolated per
row, the second stops the run before any row of that file is loaded.

Usage
    python scripts/test_harness.py [--admin-dsn "host=localhost user=postgres dbname=postgres"]
                           [--db-name etl_harness] [--engine scripts/etl_engine.py]
                           [--config config/pipeline_config.yaml] [--schema scripts/schema.sql]
                           [--drop-db]

    Run from the repository root. By default the harness uses scripts/etl_engine.py,
    scripts/schema.sql and config/pipeline_config.yaml, so only --admin-dsn is needed.

The admin DSN must be allowed to CREATE and DROP the harness database. It is
read from --admin-dsn or the environment variable ETL_PG_ADMIN_DSN. The
harness only ever drops a database whose name starts with "etl_harness".

Exit code: 0 if every check passed, 1 otherwise.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import psycopg2
import yaml
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, make_dsn

HERE = Path(__file__).resolve().parent
WIDTH = 78

ORDER_HEADER = [
    "order_id", "line_number", "order_ts", "customer_id", "customer_email", "customer_name",
    "ship_country", "ship_postcode", "product_sku", "product_name", "category", "quantity",
    "unit_price", "discount_amount", "shipping_amount", "total_amount", "currency_code",
]

ALERT_TYPE = ("⚠️ [DATA QUALITY ALERT] Non-recoverable Data Type Anomaly Injected! "
              "Safely routing malformed row to 'etl.quarantine_staging' table...")
ALERT_NULL = ("⚠️ [DATA QUALITY ALERT] Mandatory Value Missing! "
              "Safely routing incomplete row to 'etl.quarantine_staging' table...")
ALERT_CONTRACT = ("\U0001f6a8 [CRITICAL INFRASTRUCTURE ALERT] Mandatory Column Violation Detected! "
                  "Aborting file load and triggering database ROLLBACK block...")


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

def banner(title: str, char: str = "=") -> None:
    print()
    print(char * WIDTH)
    print(f"{char}{char} {title}")
    print(char * WIDTH)


def framed(lines: list[str], title: str) -> None:
    inner = WIDTH - 4
    print("+" + "-" * (WIDTH - 2) + "+")
    print("| " + title.ljust(inner) + " |")
    print("+" + "-" * (WIDTH - 2) + "+")
    for line in lines or ["(no output)"]:
        wrapped = textwrap.wrap(line, width=inner, subsequent_indent="    ",
                                break_long_words=True, replace_whitespace=False) or [""]
        for part in wrapped:
            print("| " + part.ljust(inner) + " |")
    print("+" + "-" * (WIDTH - 2) + "+")


@dataclass
class Checks:
    results: list[tuple[str, bool, str]] = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        self.results.append((name, ok, detail))
        mark = "PASS" if ok else "FAIL"
        print(f"  [{mark}] {name}" + (f"  ({detail})" if detail else ""))
        return ok

    @property
    def passed(self) -> bool:
        return all(ok for _, ok, _ in self.results)


# ---------------------------------------------------------------------------
# Mock data
# ---------------------------------------------------------------------------

def valid_rows() -> list[list[str]]:
    """Eight valid order lines; totals satisfy the total_arithmetic rule."""
    rows = []
    for i in range(1, 9):
        qty, price, discount, shipping = i % 3 + 1, 12.50 + i, 0.00 if i % 2 else 2.00, 4.90
        total = round(price * qty + shipping - discount, 2)
        cust = f"C{1000 + i % 4:05d}"
        rows.append([
            f"ORD-{2026000 + i}", "1", f"2025-03-{10 + i:02d} 14:{i * 5:02d}:00", cust,
            f"{cust.lower()}@example.com", f"Customer {cust}", "DE", f"{10115 + i}",
            f"SKU-{100 + i % 3}", f"Product {100 + i % 3}", "Kitchen", str(qty),
            f"{price:.2f}", f"{discount:.2f}", f"{shipping:.2f}", f"{total:.2f}", "EUR",
        ])
    return rows


def mock_orders() -> list[list[str]]:
    rows = valid_rows()
    corrupt = ["ORD-2026009", "1", "2025-03-19 15:00:00", "C01009", "c01009@example.com",
               "Customer C01009", "DE", "10124", "SKU-101", "Product 101", "Kitchen", "2",
               "15.00", "0.00", "4.90", "CRITICAL_ERR", "EUR"]
    blank_customer = ["ORD-2026010", "1", "2025-03-20 16:00:00", "", "c01010@example.com",
                      "Customer C01010", "DE", "10125", "SKU-102", "Product 102", "Kitchen", "1",
                      "20.00", "0.00", "4.90", "24.90", "EUR"]
    return rows + [corrupt, blank_customer]


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def write_contract_violation(path: Path) -> None:
    """Same valid rows, but the mandatory column customer_id is absent."""
    drop = ORDER_HEADER.index("customer_id")
    header = [c for i, c in enumerate(ORDER_HEADER) if i != drop]
    rows = [[v for i, v in enumerate(r) if i != drop] for r in valid_rows()]
    write_csv(path, header, rows)


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def create_database(admin_dsn: str, db_name: str, schema_path: Path) -> str:
    if not re.fullmatch(r"etl_harness[a-z0-9_]*", db_name):
        raise SystemExit("refusing to use a database name that does not start with 'etl_harness'")
    admin = psycopg2.connect(admin_dsn)
    admin.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with admin.cursor() as cur:
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    admin.close()
    dsn = make_dsn(admin_dsn, dbname=db_name)
    conn = psycopg2.connect(dsn)
    with conn, conn.cursor() as cur:
        cur.execute(schema_path.read_text(encoding="utf-8"))
    conn.close()
    return dsn


def drop_database(admin_dsn: str, db_name: str) -> None:
    admin = psycopg2.connect(admin_dsn)
    admin.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with admin.cursor() as cur:
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
    admin.close()


def harness_config(base_config: Path, work: Path) -> Path:
    """Copy the production configuration, redirecting inbox and archive to the temp dir."""
    cfg = yaml.safe_load(base_config.read_text(encoding="utf-8"))
    cfg["pipeline"]["paths"]["inbox"] = str(work / "inbox")
    cfg["pipeline"]["paths"]["archive"] = str(work / "archive")
    path = work / "pipeline_config.harness.yaml"
    path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Engine execution
# ---------------------------------------------------------------------------

def run_engine(engine: Path, config: Path, dsn: str, work: Path) -> tuple[int, list[str]]:
    env = dict(os.environ)
    env["ETL_PG_DSN"] = dsn
    env["ETL_PII_SALT"] = os.environ["ETL_PII_SALT"]
    proc = subprocess.run(
        [sys.executable, str(engine), "--config", str(config), "--log-level", "INFO"],
        cwd=work, env=env, capture_output=True, text=True, encoding="utf-8",
    )
    stream = (proc.stdout + proc.stderr).splitlines()
    framed(stream, f"etl_engine.py output (exit code {proc.returncode})")
    return proc.returncode, stream


def fetch_log(cur, source_file: str) -> dict | None:
    cur.execute("""SELECT log_id, run_id::TEXT, status, rows_read, rows_inserted, rows_quarantined,
                          message, execution_time
                   FROM etl.pipeline_logs WHERE source_file = %s
                   ORDER BY log_id DESC LIMIT 1""", (source_file,))
    row = cur.fetchone()
    if row is None:
        return None
    keys = ["log_id", "run_id", "status", "rows_read", "rows_inserted", "rows_quarantined",
            "message", "execution_time"]
    return dict(zip(keys, row))


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def scenario_a(engine: Path, config: Path, dsn: str, work: Path, checks: Checks) -> None:
    banner("SCENARIO A: row-level isolation (orders_mock.csv, 10 records)")
    inbox = work / "inbox"
    write_csv(inbox / "orders_mock.csv", ORDER_HEADER, mock_orders())
    print("  Mock file written: 8 valid rows, row 9 total_amount='CRITICAL_ERR', "
          "row 10 customer_id=''")

    code, _ = run_engine(engine, config, dsn, work)
    checks.check("engine exit code is 0", code == 0, f"exit {code}")

    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        log = fetch_log(cur, "orders_mock.csv")
        if not checks.check("pipeline_logs entry exists", log is not None):
            return
        cur.execute("""SELECT source_row_number, error_code, error_message, raw_record
                       FROM etl.quarantine_staging WHERE log_id = %s
                       ORDER BY source_row_number""", (log["log_id"],))
        quarantined = cur.fetchall()
        cur.execute("SELECT count(*) FROM dwh.fact_order_line WHERE run_id = %s", (log["run_id"],))
        facts = cur.fetchone()[0]

    print()
    for row_number, error_code, message, _ in quarantined:
        if error_code == "TYPE_ERROR":
            print(f"  {ALERT_TYPE}")
        elif error_code == "NULL_VIOLATION":
            print(f"  {ALERT_NULL}")
        else:
            print(f"  [DATA QUALITY ALERT] {error_code} on record {row_number}")
        print(f"      record {row_number}: {error_code} - {message}")
    print()

    by_row = {r[0]: r[1] for r in quarantined}
    checks.check("status is SUCCESS", log["status"] == "SUCCESS", log["status"])
    checks.check("record 9 quarantined as TYPE_ERROR", by_row.get(9) == "TYPE_ERROR",
                 str(by_row.get(9)))
    checks.check("record 10 quarantined as NULL_VIOLATION", by_row.get(10) == "NULL_VIOLATION",
                 str(by_row.get(10)))
    checks.check("no valid record quarantined", set(by_row) == {9, 10}, f"rows {sorted(by_row)}")
    checks.check("8 fact rows loaded for this run", facts == 8, f"{facts} rows")

    clear_pii = []
    for row_number, _, _, raw in quarantined:
        record = raw if isinstance(raw, dict) else json.loads(raw)
        text = json.dumps(record)
        if "customer_name" in record or "customer_email" in record or "@" in text:
            clear_pii.append(row_number)
    checks.check("quarantined records contain no clear personal data", not clear_pii,
                 "pseudonymised" if not clear_pii else f"rows {clear_pii}")
    hashes = [r[3].get("customer_hash") for r in quarantined if isinstance(r[3], dict)]
    checks.check("customer_id replaced by HMAC-SHA-256 hash",
                 any(h and re.fullmatch(r"[0-9a-f]{64}", h) for h in hashes))

    read, ins, quar = log["rows_read"], log["rows_inserted"], log["rows_quarantined"]
    balanced = read == ins + quar
    framed([
        f"source file        : orders_mock.csv",
        f"status             : {log['status']}",
        f"rows_read          : {read}",
        f"rows_inserted      : {ins}",
        f"rows_quarantined   : {quar}  (quarantine_staging holds {len(quarantined)} rows)",
        f"execution_time     : {log['execution_time']} s",
        "",
        f"reconciliation     : {read} = {ins} + {quar}  ->  {'HOLDS' if balanced else 'BROKEN'}",
    ], "RECONCILIATION SUMMARY (read from etl.pipeline_logs)")
    checks.check("rows_read (10) = rows_inserted (8) + rows_quarantined (2)",
                 (read, ins, quar) == (10, 8, 2) and balanced, f"{read} = {ins} + {quar}")
    checks.check("quarantine count matches the log", quar == len(quarantined))
    checks.check("processed file archived",
                 not (work / "inbox" / "orders_mock.csv").exists()
                 and any(p.name.endswith("_orders_mock.csv") for p in (work / "archive").iterdir()))


def scenario_b(engine: Path, config: Path, dsn: str, work: Path, checks: Checks) -> None:
    banner("SCENARIO B: contract halt (mandatory column customer_id missing)")
    inbox = work / "inbox"
    target = inbox / "orders_contract_violation.csv"
    write_contract_violation(target)
    print("  Mock file written: 8 otherwise valid rows, header without customer_id")

    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dwh.fact_order_line")
        facts_before = cur.fetchone()[0]

    code, _ = run_engine(engine, config, dsn, work)

    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        log = fetch_log(cur, target.name)
        cur.execute("SELECT count(*) FROM dwh.fact_order_line")
        facts_after = cur.fetchone()[0]
        quarantined = 0
        if log:
            cur.execute("SELECT count(*) FROM etl.quarantine_staging WHERE log_id = %s",
                        (log["log_id"],))
            quarantined = cur.fetchone()[0]

    halted = code == 2 and log is not None and log["status"] == "FAILED" \
        and "CONTRACT_VIOLATION" in (log["message"] or "")
    if halted:
        print()
        print(f"  {ALERT_CONTRACT}")
        print(f"      {log['message']}")
        print()

    checks.check("engine exit code is 2 (contract violation)", code == 2, f"exit {code}")
    checks.check("pipeline_logs status is FAILED with CONTRACT_VIOLATION", halted,
                 log["message"] if log else "no log entry")
    checks.check("rows_inserted = 0 (file transaction rolled back)",
                 log is not None and log["rows_inserted"] == 0)
    checks.check("no fact rows added", facts_after == facts_before,
                 f"{facts_before} before, {facts_after} after")
    checks.check("nothing written to quarantine for the halted file", quarantined == 0)
    checks.check("file left in inbox for a retry", target.exists())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="End-to-end verification of etl_engine.py")
    parser.add_argument("--admin-dsn", default=os.environ.get("ETL_PG_ADMIN_DSN", "dbname=postgres"))
    parser.add_argument("--db-name", default="etl_harness")
    parser.add_argument("--engine", type=Path, default=HERE / "etl_engine.py")
    parser.add_argument("--config", type=Path, default=HERE.parent / "config" / "pipeline_config.yaml")
    parser.add_argument("--schema", type=Path, default=HERE / "schema.sql")
    parser.add_argument("--drop-db", action="store_true",
                        help="drop the harness database at the end (default: keep it for inspection)")
    args = parser.parse_args()
    # Resolve against the current directory now: the engine later runs inside a temp directory.
    args.engine, args.config, args.schema = (p.resolve() for p in (args.engine, args.config, args.schema))

    # Demonstration salt for the HMAC-SHA-256 pseudonymisation layer.
    # A production deployment reads its own secret from the environment instead.
    os.environ["ETL_PII_SALT"] = "IU_Master_Secret_Salt_2026"

    for path in (args.engine, args.config, args.schema):
        if not path.exists():
            print(f"missing file: {path}", file=sys.stderr)
            return 1

    banner("ETL TEST HARNESS: configuration-driven batch pipeline")
    print(f"  engine : {args.engine}")
    print(f"  config : {args.config}")
    print(f"  schema : {args.schema}")
    print(f"  db     : {args.db_name} (recreated from schema.sql)")

    checks = Checks()
    dsn = create_database(args.admin_dsn, args.db_name, args.schema)
    try:
        with tempfile.TemporaryDirectory(prefix="etl_harness_") as tmp:
            work = Path(tmp)
            (work / "inbox").mkdir()
            (work / "archive").mkdir()
            config = harness_config(args.config, work)
            scenario_a(args.engine, config, dsn, work, checks)
            scenario_b(args.engine, config, dsn, work, checks)
    finally:
        if args.drop_db:
            drop_database(args.admin_dsn, args.db_name)

    total = len(checks.results)
    failed = [name for name, ok, _ in checks.results if not ok]
    banner(f"RESULT: {total - len(failed)} of {total} checks passed", "#")
    for name in failed:
        print(f"  FAILED: {name}")
    if not args.drop_db:
        print(f"  Database '{args.db_name}' kept for inspection.")
    return 0 if checks.passed else 1


if __name__ == "__main__":
    sys.exit(main())
