# Chapter 5 reproducibility package

Scripts and reference results for Chapter 5 of the Master's thesis *Development and Evaluation of a
Configuration-Driven ETL Pipeline for E-Commerce Data Integration and Analytics* (Kamilla Garifullina,
IU International University of Applied Sciences). Code repository:
<https://github.com/dataenglab/ecommerce-metadata-driven-etl>.

## Contents

| Path | Purpose | Thesis section |
| --- | --- | --- |
| `pipeline/etl_engine.py`, `pipeline/pipeline_config.yaml`, `pipeline/schema.sql` | The evaluated artifact (identical to Appendices B–D) | Ch. 4 |
| `generate_data.py` | Synthetic tiers with injected defects and an injection log (seed 42) | 3.5, 5.1.2 |
| `run_benchmark.py`, `run_all.py` | Five repetitions per tier; writes `results/results.jsonl` | 5.1–5.3 |
| `summarise.py` | Tables 16–19 from a results file | 5.2–5.3 |
| `classify.py` | Explains every non-injected quarantine record | 5.3.2 |
| `rq3/` | Change scenarios S1–S4 and the hard-coded baseline | 5.4 |
| `analytical_queries.sql`, `run_queries.py` | Queries Q1–Q4 and their timing | 5.6 |
| `pgutil.py` | Shared database helpers (connection, database reset, server CPU) | — |
| `reference_results/` | The results reported in the thesis | Ch. 5 |

`reference_results/` holds the runs reported in Chapter 5, carried out in October 2026 on a laptop with an Intel Core
i3-1115G4 (2 cores, 4 threads), 8 GB RAM and Windows 11 Home (version 25H2, 64-bit), with PostgreSQL 16 in Docker, Python 3.12 and
pandas 2.3.3: 20 benchmark runs, one extra Stress-tier load with `classify.py` and the Section 5.6 queries,
and the RQ3 scenarios.

## Requirements

* Python 3.12 or later
* PostgreSQL 16, for example the Docker container from the thesis guide:
  `docker run --name iu-thesis-postgres -e POSTGRES_PASSWORD=<password> -p 5433:5432 -d postgres:16`
* `pip install -r requirements.txt`
* About 3 GB of free RAM for the Stress tier (1,000,000 rows per dataset)

## Configuration

All scripts read one environment variable. It must point to the `postgres` maintenance database of a
server that holds **no data you need**: the scripts drop and recreate the working databases `bench`
and `rq3`.

```bash
# Git Bash on Windows, or any Unix shell
export BENCH_ADMIN_DSN="host=localhost port=5433 user=postgres dbname=postgres password=<password>"
export PG_CONTAINER=iu-thesis-postgres     # optional: measure PostgreSQL CPU inside the container
```

Without `PG_CONTAINER` (and not on Linux with a local server), the PostgreSQL CPU column is recorded
as `null`; all other metrics are unaffected.

## Running

```bash
python run_all.py                              # 4 tiers x 5 repetitions, about 1-1.5 h on a 2-core laptop
python summarise.py                            # tables from results/results.jsonl
python summarise.py reference_results/results.jsonl   # the same tables for the reported runs

python run_benchmark.py --tier tiers/tier_1000000 --reps 1 --results results/stress_once.jsonl
python classify.py tiers/tier_1000000          # all 349 non-injected records: DST-ambiguous hour
python run_queries.py                          # Section 5.6, on the Stress warehouse just loaded

python rq3/rq3_scenarios.py                    # needs tiers/tier_10000; writes results/rq3_results.json
```

Quick check before a full run: `python run_all.py --tiers 10000 50000 --reps 2` (about a minute).


## Supplementary run: UTC timestamps in the source files

The reported runs use local Europe/Berlin timestamps without an offset for orders and half of the
clickstream events, as a storefront export typically delivers them. This produced the false quarantines of
Section 5.3.2: in the hour that occurs twice when clocks go back, a local time cannot be converted to UTC.

`run_utc_check.py` repeats the evaluation once per tier with every source timestamp in UTC:

```bash
python run_utc_check.py                 # all four tiers, one repetition each
python run_utc_check.py --tiers 10000   # Small tier only
```

- `generate_data.py --utc` writes order and clickstream timestamps as ISO 8601 UTC (`...Z`). The random
  sequence is unchanged, so records, injected defects and the injection log are identical to the
  default run; only the timestamp strings differ.
- `pipeline/pipeline_config_utc.yaml` is identical to `pipeline/pipeline_config.yaml` except that
  `order_ts` is declared as `iso8601`. The engine is unchanged.
- Results go to `results/utc_check.jsonl`; the script prints injected, correctly coded, missed and
  falsely quarantined records per tier.

## What should match exactly, and what will differ

| Result | Expected on another machine |
| --- | --- |
| Injected counts (600 / 3,004 / 6,009 / 60,099), quarantine codes, the 398 false quarantines, reconciliation of every log entry | Identical (fixed seed, deterministic engine) |
| RQ3 line counts and before/after behavior (Table 20) | Identical |
| Q1–Q4 result row counts (96 / 10 / 24 / 12) and the Q1 EUR total | Identical |
| Wall time, throughput, CPU %, peak memory, query times | Environment-dependent |

A difference in a deterministic result points to different library versions (for example time-zone
handling in pandas); record `pip freeze` with your results.
