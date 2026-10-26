"""summarise.py - aggregates a results.jsonl file (default: results/results.jsonl; pass a path to
summarise another, e.g. reference_results/results.jsonl) into the Chapter 5 tables (mean, median, min-max over repetitions)."""
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

src = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "results" / "results.jsonl"
rows = [json.loads(l) for l in open(src, encoding="utf-8")]
by_tier = defaultdict(list)
for r in rows:
    by_tier[r["tier"]].append(r)


def fmt(vals, d=1):
    vals = [v for v in vals if v is not None]
    if not vals:
        return "n/a"
    return f"{st.mean(vals):.{d}f} | {st.median(vals):.{d}f} | {min(vals):.{d}f}-{max(vals):.{d}f}"


order = sorted(by_tier, key=lambda t: int(t.split("_")[1]))
print("TIER | n reps | wall s (mean|median|range) | engine CPU % | DB CPU % | peak RSS MB | exits | all success | reconciled")
for t in order:
    rs = by_tier[t]
    wall = [r["wall_s"] for r in rs]
    cpu = [100 * r["engine_cpu_s"] / r["wall_s"] for r in rs]
    db = [None if r["db_cpu_s"] is None else 100 * r["db_cpu_s"] / r["wall_s"] for r in rs]
    rss = [r["peak_rss_mb"] for r in rs]
    ok = all(all(d["all_success"] for d in r["datasets"].values()) for r in rs)
    rec = all(all(d["reconciled"] for d in r["datasets"].values()) for r in rs)
    print(t, len(rs), fmt(wall), fmt(cpu, 0), fmt(db, 0), fmt(rss, 0), {r["exit_code"] for r in rs}, ok, rec)

print("\nTHROUGHPUT rows/s per dataset (rows_read / sum execution_time): mean | median | range ; exec_s mean")
for t in order:
    rs = by_tier[t]
    for ds in ["orders", "clickstream", "payments"]:
        tp = [r["datasets"][ds]["read"] / r["datasets"][ds]["exec_s"] for r in rs]
        ex = [r["datasets"][ds]["exec_s"] for r in rs]
        d0 = rs[0]["datasets"][ds]
        print(t, ds, fmt(tp, 0), f"exec {st.mean(ex):.2f}s", f"files {d0['files']} read {d0['read']} ins {d0['inserted']} quar {d0['quarantined']}")
    total_rows = sum(rs[0]["datasets"][d]["read"] for d in rs[0]["datasets"])
    print(t, "ALL", fmt([total_rows / r["wall_s"] for r in rs], 0), "rows/s end-to-end, total rows", total_rows)

print("\nQUALITY (rep 1)")
for t in order:
    q = by_tier[t][0]["quality"]
    total = sum(by_tier[t][0]["datasets"][d]["read"] for d in by_tier[t][0]["datasets"])
    clean = total - q["injected"]
    print(t, f"injected {q['injected']} correct {q['correct']} wrong {q['wrong_code']} missed {q['missed']} "
             f"non-injected quarantined {q['not_injected_quarantined']} of {clean} clean "
             f"({100*q['not_injected_quarantined']/clean:.4f}%)")
    print("   by code:", json.dumps(q["by_code"]))
    print("   extra samples:", q["samples_extra"][:3])
