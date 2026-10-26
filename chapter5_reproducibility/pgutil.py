"""pgutil.py - database helpers shared by the Chapter 5 scripts.

Connection settings come from one environment variable, BENCH_ADMIN_DSN, which must point to
an administrative database (normally `postgres`) of a PostgreSQL 16 server, for example

    BENCH_ADMIN_DSN="host=localhost port=5433 user=postgres dbname=postgres password=..."

The scripts create and drop their own working databases (`bench`, `rq3`) on that server.
Never point BENCH_ADMIN_DSN at a server that holds data you need: the working databases are
dropped and recreated before every run.

PostgreSQL server CPU time is optional. It is read from /proc of the server processes:
  * set PG_CONTAINER=<name> when PostgreSQL runs in a Docker container (e.g. iu-thesis-postgres);
  * otherwise, on Linux with PostgreSQL on the same host, local /proc is used;
  * on any other set-up the value is recorded as null.
"""
import os
import platform
import subprocess
from pathlib import Path

import psycopg2
from psycopg2.extensions import make_dsn

HERE = Path(__file__).resolve().parent
PIPELINE = HERE / "pipeline"
ENGINE = PIPELINE / "etl_engine.py"
SCHEMA = PIPELINE / "schema.sql"
CONFIG = PIPELINE / "pipeline_config.yaml"


def admin_dsn() -> str:
    dsn = os.environ.get("BENCH_ADMIN_DSN")
    if not dsn:
        raise SystemExit("Set BENCH_ADMIN_DSN, e.g. "
                         "BENCH_ADMIN_DSN=\"host=localhost port=5433 user=postgres dbname=postgres password=...\"")
    return dsn


def dsn_for(db: str) -> str:
    """DSN of a working database on the same server as BENCH_ADMIN_DSN."""
    return make_dsn(admin_dsn(), dbname=db)


def recreate_db(db: str, extra_ddl: str = "") -> None:
    """Drop and recreate a working database, then build the warehouse from schema.sql."""
    if db in ("postgres", "template0", "template1"):
        raise ValueError("refusing to drop a system database")
    conn = psycopg2.connect(admin_dsn())
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()", (db,))
        cur.execute(f'DROP DATABASE IF EXISTS "{db}"')
        cur.execute(f'CREATE DATABASE "{db}"')
    conn.close()
    conn = psycopg2.connect(dsn_for(db))
    with conn, conn.cursor() as cur:
        cur.execute(SCHEMA.read_text(encoding="utf-8"))
        if extra_ddl:
            cur.execute(extra_ddl)
    conn.close()


def query(db: str, sql: str, params=None) -> list[tuple]:
    conn = psycopg2.connect(dsn_for(db))
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def _sum_postgres_ticks(text: str) -> int:
    """Parse pairs of lines '<comm>' / '<stat>' and sum utime+stime+cutime+cstime of postgres processes."""
    lines = text.splitlines()
    total = 0
    for comm, stat in zip(lines[0::2], lines[1::2]):
        if not (comm.startswith("postgres") or comm.startswith("postmaster")):
            continue
        fields = stat.rsplit(")", 1)[-1].split()
        if len(fields) > 14:
            total += sum(int(fields[i]) for i in (11, 12, 13, 14))
    return total


def pg_cpu_seconds():
    """Cumulative CPU seconds of all PostgreSQL server processes, or None if not measurable here."""
    container = os.environ.get("PG_CONTAINER")
    script = 'for d in /proc/[0-9]*; do cat "$d/comm" "$d/stat" 2>/dev/null; done'
    if container:
        out = subprocess.run(["docker", "exec", container, "sh", "-c", script + "; getconf CLK_TCK"],
                             capture_output=True, text=True)
        if out.returncode != 0:
            return None
        body, _, tick = out.stdout.rstrip("\n").rpartition("\n")
        return _sum_postgres_ticks(body) / int(tick)
    if platform.system() == "Linux":
        out = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return _sum_postgres_ticks(out.stdout) / os.sysconf("SC_CLK_TCK")
    return None
