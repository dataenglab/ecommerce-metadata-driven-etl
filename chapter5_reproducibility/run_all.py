"""run_all.py - regenerates the four volume tiers and runs five repetitions of each.

    python run_all.py                 # all tiers, 5 repetitions each (about 40 minutes)
    python run_all.py --tiers 10000 50000 --reps 2    # quick check

Results are appended to results/results.jsonl (deleted at the start of a full run).
"""
import argparse
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

ap = argparse.ArgumentParser()
ap.add_argument("--tiers", nargs="+", type=int, default=[10000, 50000, 100000, 1000000])
ap.add_argument("--reps", type=int, default=5)
a = ap.parse_args()

results = HERE / "results" / "results.jsonl"
results.parent.mkdir(exist_ok=True)
results.unlink(missing_ok=True)
for n in a.tiers:
    tier = HERE / "tiers" / f"tier_{n}"
    if not tier.is_dir():
        subprocess.run([sys.executable, str(HERE / "generate_data.py"), "--rows", str(n), "--out", str(tier)],
                       check=True)
    subprocess.run([sys.executable, str(HERE / "run_benchmark.py"), "--tier", str(tier), "--reps", str(a.reps),
                    "--results", str(results)], check=True)
print("ALL_DONE")
