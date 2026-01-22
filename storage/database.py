from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base, Loan, RejectedLoan
from typing import List, Dict
from datetime import datetime


DB_PATH = "sqlite:///data/processed/etl_pipeline.db"


def get_engine():
    return create_engine(DB_PATH)


def create_tables():
    engine = get_engine()
    Base.metadata.create_all(engine)


def insert_clean_records(records: List[Dict]):
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    for r in records:
        loan = Loan(
            loan_id=r["loan_id"],
            borrower_name=r["borrower_name"],
            loan_amount=float(r["loan_amount"]),
            loan_status=r["loan_status"],
            open_date=datetime.strptime(r["open_date"], "%Y-%m-%d").date(),
            client_id=r["client_id"],
            ingestion_id=r["ingestion_id"],
            ingestion_timestamp=datetime.fromisoformat(r["ingestion_timestamp"])
        )
        session.add(loan)

    session.commit()
    session.close()


def insert_rejected_records(records: List[Dict]):
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    for r in records:
        rejected = RejectedLoan(
            loan_id=r.get("loan_id", "UNKNOWN"),
            borrower_name=r.get("borrower_name"),
            loan_amount=r.get("loan_amount"),
            loan_status=r.get("loan_status"),
            open_date=r.get("open_date"),
            client_id=r.get("client_id"),
            ingestion_id=r.get("ingestion_id"),
            ingestion_timestamp=r.get("ingestion_timestamp"),
            rejection_reason=r.get("rejection_reason", "Unknown")
        )
        session.add(rejected)

    session.commit()
    session.close()
