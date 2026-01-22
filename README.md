# ETL-Pipeline
Lender Data Integration & Validation System

## Problem Statement:
Financial and insurance companies receive client data in inconsistent formats with missing fields, invalid values, and unclear business rules. Manual fixes are slow and error-prone. This project simulates a production-style data integration pipeline that ingests lender files, validates and transforms data, tracks quality issues, and delivers analytics for operational decision-making.

## How to Run

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd ETL-Pipeline
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### Running the ETL Pipeline

The main entry point for the pipeline is the ingestion module. It requires a client name and input file path.

**Basic Usage:**
```bash
python ingestion/ingest.py --client <client_name> --file <input_file_path>
```

**Example:**
```bash
python ingestion/ingest.py --client lender_a --file data/raw/lender_a/sample.csv
python ingestion/ingest.py --client lender_b --file data/raw/lender_b/sample.csv
```

**Supported Clients:**
- `lender_a` - Lender A configuration
- `lender_b` - Lender B configuration
- `lender_c` - Lender C configuration

### Pipeline Output

The pipeline generates the following outputs:

1. **Processed Data:**
   - Clean records: `data/processed/loans_clean.csv`
   - Rejected records: `data/rejected/loans_error.csv`

2. **Logs:**
   - Ingestion logs: `logs/ingestion.log`
   - Error logs: `logs/error.log`

3. **Console Reports:**
   - Quality metrics report (pass/fail rates)
   - Business summary report

### Running Tests

Execute the test suite to validate the pipeline:
```bash
pytest tests/
```

Or run specific test files:
```bash
pytest tests/test_validation.py
pytest tests/test_transformation.py
```
