# ecommerce-metadata-driven-etl
A lightweight, configuration-driven ETL pipeline for SME e-commerce analytics using Python and PostgreSQL. Developed as part of a Master's Thesis at IU International University.

This repository contains the production-grade programmatic artifact developed as part of the M.Sc. Data Engineering Master's Thesis at **IU International University of Applied Sciences**.

The system operationalizes a robust, transactional, and metadata-driven Batch ETL framework designed specifically for resource-constrained small and medium-sized e-commerce enterprises (SMEs).

## 🚀 Key Architectural Features
- **Declarative Quality Control (YAML):** Complete structural pipeline control and data-type validation driven by centralized metadata configurations without hardcoded Python regressions.
- **Graceful Fault Isolation (Quarantine Management):** Automatic isolation of malformed transactional records into an explicit `quarantine_staging` storage layer, preventing complete batch execution halts while maintaining absolute financial reconciliation balance (`rows_read = rows_inserted + rows_quarantined`).
- **Cryptographic Privacy Compliance (GDPR Enforcement):** In-stream pipeline pseudonymization applying keyed **HMAC-SHA-256** signatures to Personable Identifiable Information (PII) before database commits.
- **Empirical Scale Resilience:** Proven performance linear scalability stress-tested up to **1,000,000 operational rows** on standard cost-effective SME infrastructure profiles.

## 🛠️ Technological Stack
- **Core Engine:** Python 3.13
- **Storage & Relational Warehouse:** PostgreSQL 16 (Structured via Kimball Analytical Star Schema)
- **Database Connector:** psycopg2 (Chunk-based batch transaction mapping)
- **Metadata Parser:** PyYAML 6.0

## 📂 Repository Structure
- `config/pipeline_config.yaml`: Centralized declarative parameters, schemas validation maps, and GDPR constraints.
- `scripts/schema.sql`: Production DDL definitions for the analytical target star schema, system telemetry logs, and fault quarantine tables.
- `src/etl_engine.py`: Complete Python ETL execution engine containing chunk processing, state management, and isolation routing blocks.

---
*Developed strictly under the Design Science Research Methodology (DSRM) framework (Peffers et al., 2007).*
