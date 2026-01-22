from datetime import datetime, UTC
from typing import List, Dict


def convert_format_string(fmt: str) -> str:
    # If already in Python format, return as-is
    if "%" in fmt:
        return fmt
    
    fmt = fmt.replace("YYYY", "%Y")
    fmt = fmt.replace("YY", "%y")
    fmt = fmt.replace("MM", "%m")
    fmt = fmt.replace("DD", "%d")
    fmt = fmt.replace("M", "%-m")  # Single M for non-padded month
    fmt = fmt.replace("D", "%-d")  # Single D for non-padded day
    # Clean up double replacements
    fmt = fmt.replace("%-m%-m", "%m")
    fmt = fmt.replace("%-d%-d", "%d")
    return fmt


def apply_mapping(record: Dict, mapping: Dict) -> Dict:
    """
    Rename fields in a record based on mapping config.

    Example mapping:
    {
      "cust_name": "borrower_name",
      "amt": "loan_amount"
    }
    """
    transformed = {}

    for src_field, target_field in mapping.items():
        transformed[target_field] = record.get(src_field)

    return transformed


def normalize_status(record: Dict, client_config: Dict) -> Dict:
    """
    Convert status codes into standardized values.
    """
    status_map = client_config.get("status_code_mapping", {})

    if "loan_status" in record:
        raw_status = record.get("loan_status")
        record["loan_status"] = status_map.get(raw_status, raw_status)

    return record


def normalize_date(record: Dict, client_config: Dict) -> Dict:
    """
    Convert date fields to ISO format (YYYY-MM-DD).
    """
    date_formats = client_config.get("date_formats", [])

    if "open_date" in record:
        raw_date = record.get("open_date")
        for fmt in date_formats:
            try:
                # Convert format string to Python strptime format
                python_fmt = convert_format_string(fmt)
                parsed = datetime.strptime(raw_date, python_fmt)
                record["open_date"] = parsed.strftime("%Y-%m-%d")
                return record
            except ValueError:
                continue

        # if conversion fails
        record["open_date"] = None
        return record


    return record


def add_metadata(record: Dict, client_config: Dict, ingestion_id: str) -> Dict:
    """
    Add metadata fields to the record.
    """
    record["client_id"] = client_config["client_id"]
    record["ingestion_id"] = ingestion_id
    record["ingestion_timestamp"] = datetime.now(UTC).isoformat()

    return record


def transform_records(
    records: List[Dict],
    mapping: Dict,
    client_config: Dict,
    ingestion_id: str
) -> List[Dict]:
    """
    Apply all transformation steps to a list of records.
    """
    transformed_records = []

    for record in records:
        # 1) rename fields
        record = apply_mapping(record, mapping)

        # 2) normalize values
        record = normalize_status(record, client_config)
        record = normalize_date(record, client_config)

        # 3) add metadata
        record = add_metadata(record, client_config, ingestion_id)

        transformed_records.append(record)

    return transformed_records
