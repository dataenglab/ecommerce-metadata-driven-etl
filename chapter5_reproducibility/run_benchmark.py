"""run_benchmark.py - executes etl_engine.py per volume tier and records the Chapter 5 metrics.

For each repetition: fresh `bench` database from schema.sql, identical input files, one engine run.
Measures wall time, engine CPU time and peak memory (inside the engine process), PostgreSQL server
CPU time (optional, see pgutil.py), per-dataset counts and execution_time from etl.pipeline_logs,
and - on the first repetition - the quarantine results against the injection log.

    python run_benchmark.py --tier tiers/tier_10000 --reps 5 --results results/results.jsonl
"""
import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from pgutil import CONFIG, ENGINE, HERE, dsn_for, pg_cpu_seconds, query, recreate_db

DB = "bench"

# Runs inside a separate Python process, so that its CPU time and peak memory are the engine's only.
WRAPPER = r"""
import runpy, sys, time
engine, config = sys.argv[1], sys.argv[2]
sys.argv = ['etl_engine.py', '--config', config, '--log-level', 'WARNING']
t = time.perf_counter(); code = 0
try:
    runpy.run_path(engine, run_name='__main__')
except SystemExit as e:
    code = e.code or 0
wall = time.perf_counter() - t
try:
    import resource
    r = resource.getrusage(resource.RUSAGE_SELF)
    cpu = r.ru_utime + r.ru_stime
    rss_mb = r.ru_maxrss / (1024 * 1024 if sys.platform == 'darwin' else 1024)
except ImportError:                       # Windows
    import psutil
    p = psutil.Process()
    ct = p.cpu_times(); cpu = ct.user + ct.system
    rss_mb = p.memory_info().peak_wset / (1024 * 1024)
print('METRICS', wall, cpu, rss_mb, code)
"""


def run_once(tier: Path, work: Path, config: Path = CONFIG) -> dict:
    shutil.rmtree(work, ignore_errors=True)
    (work / "data").mkdir(parents=True)
    shutil.copytree(tier, work / "data" / "inbox", ignore=shutil.ignore_patterns("injection_log.csv"))
    recreate_db(DB)
    env = dict(os.environ, ETL_PG_DSN=dsn_for(DB), ETL_PII_SALT="benchmark-salt")
    cpu0 = pg_cpu_seconds()
    p = subprocess.run([sys.executable, "-c", WRAPPER, str(ENGINE), str(config)], cwd=work, env=env,
                       capture_output=True, text=True)
    cpu1 = pg_cpu_seconds()
    metrics = [l for l in p.stdout.splitlines() if l.startswith("METRICS")]
    if not metrics:
        raise SystemExit("engine did not report metrics:\n" + p.stderr[-2000:])
    wall, cpu, rss_mb, exit_code = metrics[-1].split()[1:]
    datasets = {}
    for n, files, read, ins, quar, et, ok, rec in query(
            DB, "SELECT source_name, count(*), sum(rows_read), sum(rows_inserted), sum(rows_quarantined), "
                "sum(execution_time), bool_and(status = 'SUCCESS'), "
                "bool_and(rows_read = rows_inserted + rows_quarantined) "
                "FROM etl.pipeline_logs GROUP BY 1 ORDER BY 1"):
        datasets[n] = dict(files=int(files), read=int(read), inserted=int(ins), quarantined=int(quar),
                           exec_s=float(et), all_success=bool(ok), reconciled=bool(rec))
    return dict(wall_s=float(wall), engine_cpu_s=float(cpu), peak_rss_mb=float(rss_mb),
                db_cpu_s=None if cpu0 is None or cpu1 is None else cpu1 - cpu0,
                exit_code=int(exit_code), datasets=datasets, stderr=p.stderr[-500:])


def quality(tier: Path) -> dict:
    with open(tier / "injection_log.csv", encoding="utf-8") as fh:
        truth = {(r["file"], int(r["record_number"])): r["expected_code"] for r in csv.DictReader(fh)}
    got = {(f, int(rn)): (code, msg) for f, rn, code, msg in query(
        DB, "SELECT source_file, source_row_number, error_code, error_message FROM etl.quarantine_staging")}
    tp = sum(1 for k, c in truth.items() if k in got and got[k][0] == c)
    wrong = [(k, c, got[k][0]) for k, c in truth.items() if k in got and got[k][0] != c]
    missed = [(k, c) for k, c in truth.items() if k not in got]
    extra = [(k, got[k]) for k in got if k not in truth]
    by_code = {}
    for k, c in truth.items():
        d = by_code.setdefault(f"{k[0].split('_')[0]}:{c}", [0, 0])
        d[0] += 1
        d[1] += int(k in got and got[k][0] == c)
    return dict(injected=len(truth), correct=tp, wrong_code=len(wrong), missed=len(missed),
                not_injected_quarantined=len(extra), by_code=by_code,
                samples_extra=[(k, v[1][:120]) for k, v in extra[:5]],
                samples_wrong=wrong[:5], samples_missed=missed[:5])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", type=Path, required=True)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--results", type=Path, default=HERE / "results" / "results.jsonl")
    ap.add_argument("--config", type=Path, default=CONFIG, help="pipeline configuration (default: pipeline/pipeline_config.yaml)")
    a = ap.parse_args()
    a.results.parent.mkdir(parents=True, exist_ok=True)
    work = HERE / "work"
    for rep in range(1, a.reps + 1):
        res = run_once(a.tier, work, a.config.resolve())
        res.update(tier=a.tier.name, rep=rep)
        if rep == 1:
            res["quality"] = quality(a.tier)
        with open(a.results, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(res) + "\n")
        print(a.tier.name, rep, f"wall {res['wall_s']:.1f}s rss {res['peak_rss_mb']:.0f}MB exit {res['exit_code']}",
              flush=True)


if __name__ == "__main__":
    main()
