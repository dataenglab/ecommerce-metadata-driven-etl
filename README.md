# ecommerce-metadata-driven-etl

Artifact of the Master's thesis *Development and Evaluation of a Configuration-Driven ETL Pipeline for
E-Commerce Data Integration and Analytics* (Kamilla Garifullina, M.Sc. Computer Science,
IU International University of Applied Sciences, 2026).

A generic Python engine, controlled by one declarative YAML file, loads e-commerce order, clickstream and
payment files into a PostgreSQL 16 star schema with SCD Type 2 dimensions. The artifact was developed and
evaluated following the Design Science Research Methodology (Peffers et al., 2007).

## Features

- **Declarative configuration** (`config/pipeline_config.yaml`): source contracts, column types and rules,
  personal-data actions and dimensions; the engine contains no source-specific code.
- **Row-level quarantine**: defective records go to `etl.quarantine_staging` with an error code;
  the rest of the file is loaded.
- **Reconciliation**: every file satisfies rows read = rows inserted + rows quarantined, checked in Python
  and by a database constraint.
- **Pseudonymization**: customer identifiers and e-mail addresses are replaced by keyed HMAC-SHA-256 hashes
  before anything is written; names are dropped and postcodes truncated. The output is pseudonymized, not
  anonymized, personal data.
- **Audit log**: one entry per file and run in `etl.pipeline_logs`, with status, duration and row counts.

## Repository structure

| Path | Content |
| --- | --- |
| `src/etl_engine.py` | ETL engine |
| `config/pipeline_config.yaml` | Pipeline configuration |
| `scripts/schema.sql` | Warehouse DDL (PostgreSQL 16) |
| `test_harness.py` | End-to-end verification (two scenarios, 18 checks) |
| `chapter5_reproducibility/` | Data generator, benchmark and evaluation scripts, reference results (see its README) |
| `figures/` | Data and script for Figures 5–8 of the thesis |

## Requirements

- Python 3.12 or later
- `pip install pandas pyyaml psycopg2-binary` (add `matplotlib` to regenerate the figures)
- PostgreSQL 16, for example in Docker:

```bash
docker run --name iu-thesis-postgres -e POSTGRES_PASSWORD=<password> -p 5433:5432 -d postgres:16
```

## Running the verification

From the repository root:

```bash
python test_harness.py --engine src/etl_engine.py --config config/pipeline_config.yaml \
  --schema scripts/schema.sql \
  --admin-dsn "host=localhost port=5433 user=postgres dbname=postgres password=<password>"
```

Use `port=5432` if PostgreSQL is installed locally rather than in the Docker container above.
Add `--drop-db` to remove the temporary test database afterwards. Expected result:
`RESULT: 18 of 18 checks passed`.

The harness creates its own database `etl_harness`, sets a test salt for pseudonymization and runs the
unmodified engine as a subprocess. A production run reads the secret salt from the environment variable
`ETL_PII_SALT` instead.

## Reproducing Chapter 5

See `chapter5_reproducibility/README.md` for the benchmark (four volume tiers, five repetitions each),
the change-effort scenarios S1–S4 and the analytical queries Q1–Q4.

## Reproducing Figures 5–8

The CSV files in `figures/data/` contain the measured values reported in Tables 16, 17, 20 and 21 of the
thesis. To regenerate the figures:

```bash
cd figures
python make_figures.py
```
