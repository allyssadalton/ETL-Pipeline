import pytest
from unittest.mock import patch
from analytics.quality_metrics import compute_quality_metrics, print_quality_report
from analytics.reporting import (
    loan_status_report,
    average_loan_amount_by_status,
    print_business_report
)


class TestQualityMetrics:
    def test_compute_quality_metrics_all_clean(self):
        clean_records = [
            {"loan_id": "L001", "loan_status": "ACTIVE"},
            {"loan_id": "L002", "loan_status": "CLOSED"}
        ]
        rejected_records = []

        metrics = compute_quality_metrics(clean_records, rejected_records)

        assert metrics["total_records"] == 2
        assert metrics["clean_records"] == 2
        assert metrics["rejected_records"] == 0
        assert metrics["rejection_rate_percent"] == 0.0
        assert metrics["top_rejection_reasons"] == []

    def test_compute_quality_metrics_all_rejected(self):
        clean_records = []
        rejected_records = [
            {"loan_id": "L001", "rejection_reason": "Invalid data"},
            {"loan_id": "L002", "rejection_reason": "Invalid data"}
        ]

        metrics = compute_quality_metrics(clean_records, rejected_records)

        assert metrics["total_records"] == 2
        assert metrics["clean_records"] == 0
        assert metrics["rejected_records"] == 2
        assert metrics["rejection_rate_percent"] == 100.0
        assert len(metrics["top_rejection_reasons"]) == 1
        assert metrics["top_rejection_reasons"][0] == ("Invalid data", 2)

    def test_compute_quality_metrics_mixed(self):
        clean_records = [
            {"loan_id": "L001", "loan_status": "ACTIVE"}
        ]
        rejected_records = [
            {"loan_id": "L002", "rejection_reason": "Missing field"},
            {"loan_id": "L003", "rejection_reason": "Invalid format"}
        ]

        metrics = compute_quality_metrics(clean_records, rejected_records)

        assert metrics["total_records"] == 3
        assert metrics["clean_records"] == 1
        assert metrics["rejected_records"] == 2
        assert metrics["rejection_rate_percent"] == pytest.approx(66.67, rel=1e-2)
        assert len(metrics["top_rejection_reasons"]) == 2

    def test_compute_quality_metrics_empty(self):
        metrics = compute_quality_metrics([], [])

        assert metrics["total_records"] == 0
        assert metrics["clean_records"] == 0
        assert metrics["rejected_records"] == 0
        assert metrics["rejection_rate_percent"] == 0.0
        assert metrics["top_rejection_reasons"] == []

    @patch("builtins.print")
    def test_print_quality_report(self, mock_print):
        metrics = {
            "total_records": 100,
            "clean_records": 80,
            "rejected_records": 20,
            "rejection_rate_percent": 20.0,
            "top_rejection_reasons": [("Missing field", 10), ("Invalid format", 5)]
        }

        print_quality_report(metrics)

        # Verify print calls
        calls = mock_print.call_args_list
        assert len(calls) >= 5  # At least header + 4 data lines + reasons


class TestReporting:
    def test_loan_status_report(self):
        clean_records = [
            {"loan_status": "ACTIVE"},
            {"loan_status": "ACTIVE"},
            {"loan_status": "CLOSED"},
            {"loan_status": "DELINQUENT"}
        ]

        report = loan_status_report(clean_records)

        assert report["ACTIVE"] == 2
        assert report["CLOSED"] == 1
        assert report["DELINQUENT"] == 1

    def test_loan_status_report_unknown_status(self):
        clean_records = [
            {"loan_status": "ACTIVE"},
            {}  # Missing status
        ]

        report = loan_status_report(clean_records)

        assert report["ACTIVE"] == 1
        assert report["UNKNOWN"] == 1

    def test_loan_status_report_empty(self):
        report = loan_status_report([])
        assert report == {}

    def test_average_loan_amount_by_status(self):
        clean_records = [
            {"loan_status": "ACTIVE", "loan_amount": "10000"},
            {"loan_status": "ACTIVE", "loan_amount": "20000"},
            {"loan_status": "CLOSED", "loan_amount": "15000"}
        ]

        report = average_loan_amount_by_status(clean_records)

        assert report["ACTIVE"] == 15000.0
        assert report["CLOSED"] == 15000.0

    def test_average_loan_amount_by_status_empty(self):
        report = average_loan_amount_by_status([])
        assert report == {}

    @patch("builtins.print")
    def test_print_business_report(self, mock_print):
        clean_records = [
            {"loan_status": "ACTIVE", "loan_amount": "10000"},
            {"loan_status": "CLOSED", "loan_amount": "20000"}
        ]

        print_business_report(clean_records)

        # Verify print calls
        calls = mock_print.call_args_list
        assert len(calls) >= 5  # Header + status lines + avg lines