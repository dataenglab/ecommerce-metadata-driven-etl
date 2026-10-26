"""classify.py - explains every quarantine record that does not match the injection log.

Run it directly after a benchmark repetition of the tier, while the `bench` database still holds it:

    python classify.py tiers/tier_1000000
"""
import csv
import re
import sys
from collections import Counter
from pathlib import Path

from pgutil import query

tier = Path(sys.argv[1])
with open(tier / "injection_log.csv", encoding="utf-8") as fh:
    truth = {(r["file"], int(r["record_number"])): r["expected_code"] for r in csv.DictReader(fh)}
got = {(f, int(rn)): (c, m) for f, rn, c, m in query(
    "bench", "SELECT source_file, source_row_number, error_code, error_message FROM etl.quarantine_staging")}
dst = re.compile(r"2025-10-26[ T]02:")
extra, wrong = Counter(), Counter()
for k, (c, m) in got.items():
    if k not in truth:
        extra[(c, "DST-ambiguous local time 2025-10-26 02:xx" if dst.search(m) else m[:70])] += 1
for k, c in truth.items():
    if k in got and got[k][0] != c:
        m = got[k][1]
        wrong[(c, got[k][0], "DST-ambiguous timestamp detected first" if dst.search(m) else m[:70])] += 1
print("non-injected quarantined:", sum(extra.values()), dict(extra))
print("wrong code:", sum(wrong.values()), dict(wrong))
