import pytest
from datetime import datetime
from transformation.transformer import (
    convert_format_string,
    apply_mapping,
    normalize_status,
    normalize_date,
    add_metadata,
    transform_records
)


class TestConvertFormatString:
    def test_convert_python_format_unchanged(self):
        fmt = "%Y-%m-%d"
        result = convert_format_string(fmt)
        assert result == "%Y-%m-%d"

    def test_convert_custom_format(self):
        fmt = "YYYY-MM-DD"
        result = convert_format_string(fmt)
        assert result == "%Y-%m-%d"

    def test_convert_single_digit_formats(self):
        fmt = "M/D/YYYY"
        result = convert_format_string(fmt)
        assert result == "%-m/%-d/%Y"

    def test_convert_mixed_formats(self):
        fmt = "YY-MM-DD"
        result = convert_format_string(fmt)
        assert result == "%y-%m-%d"


class TestApplyMapping:
    def test_apply_mapping_basic(self):
        record = {"old_field": "value", "another": "data"}
        mapping = {"old_field": "new_field"}

        result = apply_mapping(record, mapping)
        assert result == {"new_field": "value"}

    def test_apply_mapping_multiple_fields(self):
        record = {"field1": "val1", "field2": "val2", "field3": "val3"}
        mapping = {"field1": "mapped1", "field2": "mapped2"}

        result = apply_mapping(record, mapping)
        assert result == {"mapped1": "val1", "mapped2": "val2"}

    def test_apply_mapping_missing_field(self):
        record = {"existing": "value"}
        mapping = {"missing": "target"}

        result = apply_mapping(record, mapping)
        assert result == {"target": None}


class TestNormalizeStatus:
    def test_normalize_status_with_mapping(self):
        record = {"loan_status": "A"}
        client_config = {"status_code_mapping": {"A": "ACTIVE"}}

        result = normalize_status(record, client_config)
        assert result["loan_status"] == "ACTIVE"

    def test_normalize_status_no_mapping(self):
        record = {"loan_status": "ACTIVE"}
        client_config = {}

        result = normalize_status(record, client_config)
        assert result["loan_status"] == "ACTIVE"

    def test_normalize_status_no_status_field(self):
        record = {"other_field": "value"}
        client_config = {"status_code_mapping": {"A": "ACTIVE"}}

        result = normalize_status(record, client_config)
        assert result == record


class TestNormalizeDate:
    def test_normalize_date_success(self):
        record = {"open_date": "2024-05-01"}
        client_config = {"date_formats": ["YYYY-MM-DD"]}

        result = normalize_date(record, client_config)
        assert result["open_date"] == "2024-05-01"

    def test_normalize_date_multiple_formats(self):
        record = {"open_date": "05/01/2024"}
        client_config = {"date_formats": ["MM/DD/YYYY", "YYYY-MM-DD"]}

        result = normalize_date(record, client_config)
        assert result["open_date"] == "2024-05-01"

    def test_normalize_date_invalid_format(self):
        record = {"open_date": "invalid-date"}
        client_config = {"date_formats": ["YYYY-MM-DD"]}

        result = normalize_date(record, client_config)
        assert result["open_date"] is None

    def test_normalize_date_no_date_field(self):
        record = {"other_field": "value"}
        client_config = {"date_formats": ["YYYY-MM-DD"]}

        result = normalize_date(record, client_config)
        assert result == record


class TestAddMetadata:
    def test_add_metadata(self):
        record = {"loan_id": "123"}
        client_config = {"client_id": "TEST_CLIENT"}
        ingestion_id = "INGEST_001"

        result = add_metadata(record, client_config, ingestion_id)

        assert result["client_id"] == "TEST_CLIENT"
        assert result["ingestion_id"] == "INGEST_001"
        assert "ingestion_timestamp" in result
        assert isinstance(result["ingestion_timestamp"], str)


class TestTransformRecords:
    def test_transform_records_full_pipeline(self):
        records = [
            {
                "loan_id": "L001",
                "cust_name": "John Doe",
                "amt": "15000",
                "status": "A",
                "date_opened": "2024-05-01"
            }
        ]

        mapping = {
            "cust_name": "borrower_name",
            "amt": "loan_amount",
            "status": "loan_status",
            "date_opened": "open_date"
        }

        client_config = {
            "client_id": "TEST_CLIENT",
            "status_code_mapping": {"A": "ACTIVE"},
            "date_formats": ["YYYY-MM-DD"]
        }

        ingestion_id = "INGEST_001"

        result = transform_records(records, mapping, client_config, ingestion_id)

        assert len(result) == 1
        record = result[0]
        assert record["borrower_name"] == "John Doe"
        assert record["loan_amount"] == "15000"
        assert record["loan_status"] == "ACTIVE"
        assert record["open_date"] == "2024-05-01"
        assert record["client_id"] == "TEST_CLIENT"
        assert record["ingestion_id"] == "INGEST_001"
        assert "ingestion_timestamp" in record

    def test_transform_records_empty_list(self):
        result = transform_records([], {}, {}, "INGEST_001")
        assert result == []