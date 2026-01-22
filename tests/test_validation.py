import pytest
from validation.validator import validate_record, validate_records
from validation import rules


class TestValidateRecord:
    def test_validate_record_valid(self):
        record = {
            "loan_id": "L001",
            "borrower_name": "John Doe",
            "loan_amount": 15000.0,
            "loan_status": "ACTIVE",
            "open_date": "2024-05-01",
            "client_id": "TEST",
            "ingestion_id": "INGEST_001",
            "ingestion_timestamp": "2024-01-01T00:00:00"
        }

        schema = {
            "fields": {
                "loan_id": {"type": "string", "required": True},
                "borrower_name": {"type": "string", "required": True},
                "loan_amount": {"type": "number", "required": True},
                "loan_status": {"type": "string", "required": True, "allowed_values": ["ACTIVE", "CLOSED"]},
                "open_date": {"type": "date", "required": True}
            }
        }

        client_config = {}

        is_valid, errors = validate_record(record, schema, client_config)
        assert is_valid is True
        assert errors == []

    def test_validate_record_missing_required_field(self):
        record = {
            "borrower_name": "John Doe",
            "loan_amount": 15000.0
        }

        schema = {
            "fields": {
                "loan_id": {"type": "string", "required": True},
                "borrower_name": {"type": "string", "required": True}
            }
        }

        client_config = {}

        is_valid, errors = validate_record(record, schema, client_config)
        assert is_valid is False
        assert len(errors) == 1
        assert "Missing required field: loan_id" in errors[0]

    def test_validate_record_invalid_number(self):
        record = {
            "loan_id": "L001",
            "loan_amount": "not_a_number"
        }

        schema = {
            "fields": {
                "loan_id": {"type": "string", "required": True},
                "loan_amount": {"type": "number", "required": True}
            }
        }

        client_config = {}

        is_valid, errors = validate_record(record, schema, client_config)
        assert is_valid is False
        assert any("Invalid number" in error for error in errors)

    def test_validate_record_negative_number(self):
        record = {
            "loan_id": "L001",
            "loan_amount": -100.0
        }

        schema = {
            "fields": {
                "loan_id": {"type": "string", "required": True},
                "loan_amount": {"type": "number", "required": True}
            }
        }

        client_config = {}

        is_valid, errors = validate_record(record, schema, client_config)
        assert is_valid is False
        assert any("Negative value" in error for error in errors)

    def test_validate_record_invalid_date(self):
        record = {
            "loan_id": "L001",
            "open_date": "invalid-date"
        }

        schema = {
            "fields": {
                "loan_id": {"type": "string", "required": True},
                "open_date": {"type": "date", "required": True}
            }
        }

        client_config = {}

        is_valid, errors = validate_record(record, schema, client_config)
        assert is_valid is False
        assert any("Invalid date format" in error for error in errors)

    def test_validate_record_invalid_allowed_value(self):
        record = {
            "loan_id": "L001",
            "loan_status": "INVALID_STATUS"
        }

        schema = {
            "fields": {
                "loan_id": {"type": "string", "required": True},
                "loan_status": {"type": "string", "required": True, "allowed_values": ["ACTIVE", "CLOSED"]}
            }
        }

        client_config = {}

        is_valid, errors = validate_record(record, schema, client_config)
        assert is_valid is False
        assert any("Invalid value" in error for error in errors)


class TestValidateRecords:
    def test_validate_records_all_valid(self):
        records = [
            {
                "loan_id": "L001",
                "borrower_name": "John Doe",
                "loan_amount": 15000.0,
                "loan_status": "ACTIVE",
                "open_date": "2024-05-01"
            }
        ]

        schema = {
            "fields": {
                "loan_id": {"type": "string", "required": True},
                "borrower_name": {"type": "string", "required": True},
                "loan_amount": {"type": "number", "required": True},
                "loan_status": {"type": "string", "required": True, "allowed_values": ["ACTIVE"]},
                "open_date": {"type": "date", "required": True}
            }
        }

        client_config = {}

        clean, rejected = validate_records(records, schema, client_config)
        assert len(clean) == 1
        assert len(rejected) == 0

    def test_validate_records_all_invalid(self):
        records = [
            {"loan_id": None}  # Missing required fields
        ]

        schema = {
            "fields": {
                "loan_id": {"type": "string", "required": True}
            }
        }

        client_config = {}

        clean, rejected = validate_records(records, schema, client_config)
        assert len(clean) == 0
        assert len(rejected) == 1
        assert "errors" in rejected[0]

    def test_validate_records_mixed(self):
        records = [
            {
                "loan_id": "L001",
                "borrower_name": "John Doe",
                "loan_amount": 15000.0
            },
            {
                "loan_id": None,  # Invalid
                "borrower_name": "Jane Doe"
            }
        ]

        schema = {
            "fields": {
                "loan_id": {"type": "string", "required": True},
                "borrower_name": {"type": "string", "required": True},
                "loan_amount": {"type": "number", "required": True}
            }
        }

        client_config = {}

        clean, rejected = validate_records(records, schema, client_config)
        assert len(clean) == 1
        assert len(rejected) == 1


class TestValidationRules:
    def test_required_field_valid(self):
        valid, error = rules.required_field("value", "test_field")
        assert valid is True
        assert error is None

    def test_required_field_missing(self):
        valid, error = rules.required_field(None, "test_field")
        assert valid is False
        assert error == "Missing required field: test_field"

    def test_required_field_empty_string(self):
        valid, error = rules.required_field("", "test_field")
        assert valid is False
        assert error == "Missing required field: test_field"

    def test_is_number_valid(self):
        valid, error = rules.is_number("123.45", "test_field")
        assert valid is True
        assert error is None

    def test_is_number_invalid(self):
        valid, error = rules.is_number("not_a_number", "test_field")
        assert valid is False
        assert "Invalid number" in error

    def test_non_negative_valid(self):
        valid, error = rules.non_negative("100", "test_field")
        assert valid is True
        assert error is None

    def test_non_negative_negative(self):
        valid, error = rules.non_negative("-50", "test_field")
        assert valid is False
        assert "Negative value" in error

    def test_allowed_values_valid(self):
        valid, error = rules.allowed_values("ACTIVE", "test_field", ["ACTIVE", "CLOSED"])
        assert valid is True
        assert error is None

    def test_allowed_values_invalid(self):
        valid, error = rules.allowed_values("INVALID", "test_field", ["ACTIVE", "CLOSED"])
        assert valid is False
        assert "Invalid value" in error

    def test_valid_date_valid(self):
        valid, error = rules.valid_date("2024-05-01", "test_field", ["%Y-%m-%d"])
        assert valid is True
        assert error is None

    def test_valid_date_invalid(self):
        valid, error = rules.valid_date("invalid-date", "test_field", ["%Y-%m-%d"])
        assert valid is False
        assert "Invalid date format" in error