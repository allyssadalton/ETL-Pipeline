from sqlalchemy import Column, String, Float, Date, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Loan(Base):
    __tablename__ = "loans"

    loan_id = Column(String, primary_key=True)
    borrower_name = Column(String, nullable=False)
    loan_amount = Column(Float, nullable=False)
    loan_status = Column(String, nullable=False)
    open_date = Column(Date, nullable=False)
    client_id = Column(String, nullable=False)
    ingestion_id = Column(String, nullable=False)
    ingestion_timestamp = Column(DateTime, nullable=False)


class RejectedLoan(Base):
    __tablename__ = "rejected_loans"

    loan_id = Column(String, primary_key=True)
    borrower_name = Column(String)
    loan_amount = Column(Float)
    loan_status = Column(String)
    open_date = Column(String)
    client_id = Column(String)
    ingestion_id = Column(String)
    ingestion_timestamp = Column(String)
    rejection_reason = Column(String)
