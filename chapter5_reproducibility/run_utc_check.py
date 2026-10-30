"""run_utc_check.py - supplementary run with UTC timestamps in all source files (one repetition per tier).

Generates each tier a second time with generate_data.py --utc (same seed, same records and defects;
only order and clickstream timestamps are written in UTC with a 'Z' offset), loads it with
pipeline/pipeline_config_utc.yaml and reports isolation and false quarantines next to the
reference results of the local-time run.

    python run_utc_check.py                        # all four tiers, one repetition each
    python run_utc_check.py --tiers 10000          # Small tier only (about one minute)

Results: results/utc_check.jsonl
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
ap.add_argument("--tiers", nargs="+", type=int, default=[10000, 50000, 100000, 1000000])
a = ap.parse_args()

results = HERE / "results" / "utc_check.jsonl"
results.parent.mkdir(exist_ok=True)
results.unlink(missing_ok=True)
for n in a.tiers:
    tier = HERE / "tiers" / f"tier_{n}_utc"
    if not tier.is_dir():
        subprocess.run([sys.executable, str(HERE / "generate_data.py"), "--rows", str(n), "--out", str(tier), "--utc"],
                       check=True)
    subprocess.run([sys.executable, str(HERE / "run_benchmark.py"), "--tier", str(tier), "--reps", "1",
                    "--results", str(results), "--config", str(HERE / "pipeline" / "pipeline_config_utc.yaml")],
                   check=True)

print()
print(f"{'tier':<20}{'exit':>5}{'injected':>10}{'correct code':>14}{'wrong code':>12}{'missed':>8}{'false quarantine':>18}")
for line in results.read_text(encoding="utf-8").splitlines():
    r = json.loads(line)
    q = r.get("quality", {})
    print(f"{r['tier']:<20}{r['exit_code']:>5}{q['injected']:>10}{q['correct']:>14}{q['wrong_code']:>12}"
          f"{q['missed']:>8}{q['not_injected_quarantined']:>18}")
print("UTC_CHECK_DONE")
