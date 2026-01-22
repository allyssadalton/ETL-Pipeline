# Project Architecture

This document explains the purpose of every folder and file in the **Lender Data Integration & Quality Platform**. The structure mirrors a real-world, production-style data integration pipeline used by financial and insurance companies.

---

## Root Directory

```
lender-data-platform/
```

### `README.md`

High-level overview of the project. Explains the business problem, system design, how to run the pipeline, and example outputs. Recruiters and interviewers should be able to understand the project by reading this file alone.

### `requirements.txt`

Python dependencies required to run the project (e.g., pandas, sqlalchemy, streamlit). Ensures reproducible environments.

### `.gitignore`

Specifies files and folders that should not be committed to version control (logs, local databases, virtual environments).

---

## Configuration Layer

```
config/
```

All **client-specific behavior** is defined here. No business logic lives in this folder.

### `config/clients/`

Client metadata and ingestion settings.

* `lender_a.json`
  Defines client ID, file format, delimiter, and ingestion rules.

* `lender_b.json`
  Same structure as lender A, but with different values to simulate real client variation.

### `config/schemas/`

Canonical schema definitions for standardized data.

* `loan_schema.json`
  Defines required fields, data types, and constraints for the normalized loan table.

### `config/mappings/`

Source-to-target field mappings for each client.

* `lender_a_mapping.json`
  Maps lender A column names and values to the standardized schema.

* `lender_b_mapping.json`
  Separate mapping to demonstrate multi-client support.

---

## Data Layer

```
data/
```

Stores all data artifacts generated or consumed by the pipeline.

### `data/raw/`

Immutable copies of original client files.

* `lender_a/`
  Raw CSV or JSON files received from lender A.

* `lender_b/`
  Raw files received from lender B.

These files are never modified and support auditing and reprocessing.

### `data/processed/`

Clean, standardized output data.

* `loans_clean.csv`
  Records that passed validation and transformation rules.

### `data/rejected/`

Rows that failed validation.

* `loans_error.csv`
  Includes original values and error reasons for debugging and reporting.

---

## Ingestion Layer

```
ingestion/
```

### `ingest.py`

Main pipeline entry point.

Responsibilities:

* Parse CLI arguments
* Load client configuration
* Read raw data files
* Orchestrate validation, transformation, and storage
* Generate ingestion summary and logs

This file simulates a production batch ingestion job.

---

## Validation Layer

```
validation/
```

Ensures data quality before transformation and storage.

### `validator.py`

Coordinates all validation checks. Routes records to either clean or rejected outputs.

### `rules.py`

Defines reusable validation rules such as:

* Required field checks
* Data type validation
* Allowed value enforcement
* Range constraints

---

## Transformation Layer

```
transformation/
```

### `transformer.py`

Applies business logic to validated records:

* Field renaming
* Status code normalization
* Date format conversion
* Default value handling

Transformation logic is driven by mapping configuration files.

---

## Storage Layer

```
storage/
```

### `database.py`

Handles database connections and session management.

### `models.py`

Defines database tables such as:

* Clean loans
* Error records
* Ingestion logs
* Data quality metrics

Uses a relational model to support analytics and auditing.

---

## Analytics Layer

```
analytics/
```

### `quality_metrics.py`

Computes ingestion statistics:

* Total records processed
* Rejected record counts
* Error rates
* Overall data quality score

### `reporting.py`

Generates summarized views used by dashboards and CLI output.

---

## Dashboard Layer

```
dashboard/
```

### `app.py`

Visualization layer (Streamlit or Flask).

Displays:

* Ingestion runs over time
* Error distributions
* Data quality trends by client

Designed for operational monitoring rather than visual complexity.

---

## Logging

```
logs/
```

### `ingestion.log`

High-level ingestion events and summaries.

### `error.log`

Detailed error messages and stack traces for failed records.

---

## Testing

```
tests/
```

### `test_validation.py`

Unit tests for validation rules and edge cases.

### `test_transformation.py`

Unit tests for transformation logic and mappings.

Demonstrates engineering discipline and reliability.

---

## Scripts

```
scripts/
```

### `run_ingestion.sh`

Shell script to simulate scheduled or automated ingestion runs.

Example usage:

```bash
./run_ingestion.sh lender_a loans_2026_01_21.csv
```

---

## Design Principles Highlighted

* Separation of configuration and logic
* Auditability and traceability
* Fault-tolerant ingestion
* Scalable, client-agnostic design
* Interview-ready, production-style architecture

---

This architecture is intentionally designed to resemble real financial data integration systems and to clearly demonstrate data engineering, software design, and analytics skills in interviews.
