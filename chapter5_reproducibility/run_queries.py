"""run_queries.py - times the analytical queries of Section 5.6 on the `bench` database.

Load the Stress tier once first (python run_benchmark.py --tier tiers/tier_1000000 --reps 1), then:

    python run_queries.py

Each query runs once to warm the cache, then five times under EXPLAIN (ANALYZE); the median of the
reported execution times is written to results/q56_results.json.
"""
import json
import re
import statistics
from pathlib import Path

import psycopg2

from pgutil import HERE, dsn_for

N = 5
text = (HERE / "analytical_queries.sql").read_text(encoding="utf-8")
parts = [p.strip() for p in re.split(r"(?m)^-- (Q\d):.*$", text) if p.strip()]
queries = {parts[i]: parts[i + 1].rstrip(";") for i in range(0, len(parts), 2)}

conn = psycopg2.connect(dsn_for("bench"))
conn.autocommit = True
cur = conn.cursor()
cur.execute("ANALYZE")
out = {}
for q, sql in queries.items():
    cur.execute(sql)
    rows = len(cur.fetchall())
    times = []
    for _ in range(N):
        cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql)
        times.append(cur.fetchone()[0][0]["Execution Time"] / 1000)
    out[q] = {"result_rows": rows, "runs_s": [round(t, 3) for t in times],
              "median_s": round(statistics.median(times), 3)}
    print(q, out[q])
cur.execute("SELECT sum(total_amount) FROM dwh.fact_order_line WHERE currency_code = 'EUR'")
out["check_q1_total_eur"] = str(cur.fetchone()[0])
print("Q1 cross-check, EUR total in fact table:", out["check_q1_total_eur"])
(HERE / "results").mkdir(exist_ok=True)
(HERE / "results" / "q56_results.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
