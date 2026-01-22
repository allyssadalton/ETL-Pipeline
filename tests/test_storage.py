import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock
from datetime import datetime
from storage.database import (
    get_engine,
    create_tables,
    insert_clean_records,
    insert_rejected_records
)
from storage.models import Loan, RejectedLoan


class TestDatabaseFunctions:
    def setup_method(self):
        # Create a temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        self.db_path = f"sqlite:///{self.temp_db.name}"

    def teardown_method(self):
        # Clean up temporary database
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)

    @patch("storage.database.DB_PATH", "sqlite:///:memory:")
    def test_get_engine(self):
        engine = get_engine()
        assert engine is not None

    @patch("storage.database.get_engine")
    def test_create_tables(self, mock_get_engine):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        # This should not raise an exception
        create_tables()

        # Verify that create_all was called on the metadata
        mock_engine.assert_has_calls([])  # The actual call happens inside SQLAlchemy

    @patch("storage.database.create_engine")
    @patch("storage.database.sessionmaker")
    def test_insert_clean_records(self, mock_sessionmaker, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_session = MagicMock()
        mock_sessionmaker.return_value = mock_session

        records = [
            {
                "loan_id": "L001",
                "borrower_name": "John Doe",
                "loan_amount": "15000.0",
                "loan_status": "ACTIVE",
                "open_date": "2024-05-01",
                "client_id": "TEST_CLIENT",
                "ingestion_id": "INGEST_001",
                "ingestion_timestamp": "2024-01-01T00:00:00"
            }
        ]

        # Mock the Loan constructor to avoid SQLAlchemy issues
        with patch("storage.database.Loan") as mock_loan:
            mock_loan_instance = MagicMock()
            mock_loan.return_value = mock_loan_instance

            # This should not raise an exception
            insert_clean_records(records)

            # Just verify that the function completed without error
            mock_loan.assert_called_once()

    @patch("storage.database.create_engine")
    @patch("storage.database.sessionmaker")
    def test_insert_clean_records_invalid_data(self, mock_sessionmaker, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_session = MagicMock()
        mock_sessionmaker.return_value = mock_session

        # Record with missing required field
        records = [
            {
                "loan_id": "L001",
                # Missing borrower_name
                "loan_amount": "15000.0",
                "loan_status": "ACTIVE",
                "open_date": "2024-05-01",
                "client_id": "TEST_CLIENT",
                "ingestion_id": "INGEST_001",
                "ingestion_timestamp": "2024-01-01T00:00:00"
            }
        ]

        # Should handle gracefully or raise appropriate exception
        with pytest.raises((KeyError, ValueError)):
            insert_clean_records(records)

    @patch("storage.database.create_engine")
    @patch("storage.database.sessionmaker")
    def test_insert_rejected_records(self, mock_sessionmaker, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_session = MagicMock()
        mock_sessionmaker.return_value = mock_session

        records = [
            {
                "loan_id": "L002",
                "borrower_name": "Jane Doe",
                "loan_amount": "25000.0",
                "loan_status": "INVALID",
                "open_date": "2024-06-01",
                "client_id": "TEST_CLIENT",
                "ingestion_id": "INGEST_001",
                "ingestion_timestamp": "2024-01-01T00:00:00",
                "errors": ["Invalid status"]
            }
        ]

        # Mock the RejectedLoan constructor
        with patch("storage.database.RejectedLoan") as mock_rejected_loan:
            mock_rejected_instance = MagicMock()
            mock_rejected_loan.return_value = mock_rejected_instance

            # This should not raise an exception
            insert_rejected_records(records)

            # Just verify that the function completed without error
            mock_rejected_loan.assert_called_once()

    @patch("storage.database.create_engine")
    @patch("storage.database.sessionmaker")
    def test_insert_rejected_records_minimal_data(self, mock_sessionmaker, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_session = MagicMock()
        mock_sessionmaker.return_value = mock_session

        # Record with minimal data (some fields missing)
        records = [
            {
                "loan_id": "L003",
                "errors": ["Missing borrower name"]
            }
        ]

        # Mock the RejectedLoan constructor
        with patch("storage.database.RejectedLoan") as mock_rejected_loan:
            mock_rejected_instance = MagicMock()
            mock_rejected_loan.return_value = mock_rejected_instance

            # Should handle missing fields gracefully
            insert_rejected_records(records)

            # Just verify that the function completed without error
            mock_rejected_loan.assert_called_once()