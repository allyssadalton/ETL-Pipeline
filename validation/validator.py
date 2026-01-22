from typing import List, Tuple, Dict

from validation import rules


def validate_record(
    record: Dict,
    schema: Dict,
    client_config: Dict
) -> Tuple[bool, List[str]]:
    """
    Validate a single record against the canonical schema.
    Returns (is_valid, list_of_errors).
    """
    errors = []

    for field_name, field_rules in schema["fields"].items():
        value = record.get(field_name)

        # Required check
        if field_rules.get("required"):
            valid, error = rules.required_field(value, field_name)
            if not valid:
                errors.append(error)
                continue  # no point running other rules if missing

        # Type checks
        if value is not None:
            if field_rules["type"] == "number":
                valid, error = rules.is_number(value, field_name)
                if not valid:
                    errors.append(error)
                    continue

                valid, error = rules.non_negative(value, field_name)
                if not valid:
                    errors.append(error)

            if field_rules["type"] == "date":
                valid, error = rules.valid_date(
                    value,
                    field_name,
                    client_config.get("date_formats", [])
                )
                if not valid:
                    errors.append(error)

            if "allowed_values" in field_rules:
                valid, error = rules.allowed_values(
                    value,
                    field_name,
                    field_rules["allowed_values"]
                )
                if not valid:
                    errors.append(error)

    return len(errors) == 0, errors


def validate_records(
    records: List[Dict],
    schema: Dict,
    client_config: Dict
) -> Tuple[List[Dict], List[Dict]]:
    """
    Validate a list of records.
    Returns (clean_records, rejected_records).
    """
    clean_records = []
    rejected_records = []

    for record in records:
        is_valid, errors = validate_record(
            record,
            schema,
            client_config
        )

        if is_valid:
            clean_records.append(record)
        else:
            rejected_record = record.copy()
            rejected_record["errors"] = errors
            rejected_records.append(rejected_record)

    return clean_records, rejected_records
