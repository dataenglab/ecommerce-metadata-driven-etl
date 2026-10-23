# ecommerce-metadata-driven-etl
This repository contains the programmatic artifact developed as part of the M.Sc. Data Engineering Master's Thesis at **IU International University of Applied Sciences**.

The system operationalizes a configuration-driven Batch ETL framework designed for small and medium-sized e-commerce enterprises (SMEs).

## 🚀 Key Architectural Features
- **Declarative Quality Control (YAML):** Structural validation driven by centralized metadata configurations (`config/pipeline_config.yaml`).
- **Graceful Fault Isolation (Quarantine):** Automatic routing of malformed rows into `etl.quarantine_staging` without halting execution, maintaining financial reconciliation balance.
- **GDPR Privacy Compliance:** In-stream pseudonymization applying keyed **HMAC-SHA-256** signatures to PII before database commits.
- **Automated Verification:** End-to-end reliability verification via an advanced test harness covering structural errors and critical contract violations.

## 🛠️ Technology Stack
- **Engine Layer:** Python 3.13
- **Warehouse Layer:** PostgreSQL 16 (Structured via Kimball Star Schema)
- **Database Driver:** psycopg2
- **Metadata Parser:** PyYAML 6.0

## 📂 Repository Structure
- `config/pipeline_config.yaml` - Declarative metadata validation rules.
- `scripts/schema.sql` - DDL scripts for target star schema, quarantine, and log layers.
- `src/etl_engine.py` - Core transactional Python ETL data processing engine.
- `test_harness.py` - Automated execution script verifying pipeline resilience.

## 💻 How to Run the Verification
To run the automated verification suite against a local PostgreSQL instance, execute the following command from the root directory:

```bash
python test_harness.py --admin-dsn "host=localhost user=postgres dbname=postgres password=YOUR_PASSWORD"
```
*Note: Add `--drop-db` at the end of the command if you wish to tear down the temporary testing database after execution.*
---
*Developed strictly under the Design Science Research Methodology (DSRM) framework (Peffers et al., 2007).*
