from collections import Counter
from typing import List, Dict


def loan_status_report(clean_records: List[Dict]) -> Dict:
    status_counter = Counter()

    for record in clean_records:
        status = record.get("loan_status", "UNKNOWN")
        status_counter[status] += 1

    return dict(status_counter)


def average_loan_amount_by_status(clean_records: List[Dict]) -> Dict:
    totals = {}
    counts = {}

    for record in clean_records:
        status = record.get("loan_status", "UNKNOWN")
        amount = float(record.get("loan_amount", 0))

        totals[status] = totals.get(status, 0) + amount
        counts[status] = counts.get(status, 0) + 1

    averages = {status: totals[status] / counts[status] for status in totals}
    return averages


def print_business_report(clean_records: List[Dict]):
    status_report = loan_status_report(clean_records)
    avg_amount_report = average_loan_amount_by_status(clean_records)

    print("\nBUSINESS REPORT")
    print("---------------")
    print("Loans by Status:")
    for status, count in status_report.items():
        print(f" - {status}: {count}")

    print("\nAverage Loan Amount by Status:")
    for status, avg in avg_amount_report.items():
        print(f" - {status}: ${avg:,.2f}")
