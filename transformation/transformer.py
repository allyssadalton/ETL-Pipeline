from datetime import datetime
from typing import List, Dict


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

        # TODO: Implement conversion from any allowed format to ISO
        # Hint: use datetime.strptime and datetime.strftime
        # If conversion fails, leave the raw value or set to None

    return record


def add_metadata(record: Dict, client_config: Dict, ingestion_id: str) -> Dict:
    """
    Add metadata fields to the record.
    """
    record["client_id"] = client_config["client_id"]
    record["ingestion_id"] = ingestion_id
    record["ingestion_timestamp"] = datetime.utcnow().isoformat()

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
