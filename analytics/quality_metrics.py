from collections import Counter
from typing import List, Dict


def compute_quality_metrics(clean_records: List[Dict], rejected_records: List[Dict]) -> Dict:
    total = len(clean_records) + len(rejected_records)
    clean_count = len(clean_records)
    rejected_count = len(rejected_records)

    rejection_rate = (rejected_count / total) * 100 if total else 0

    # Collect rejection reasons
    reasons = []
    for r in rejected_records:
        if "rejection_reason" in r:
            reasons.append(r["rejection_reason"])
        else:
            reasons.append("unknown")

    top_rejection_reasons = Counter(reasons).most_common(10)

    return {
        "total_records": total,
        "clean_records": clean_count,
        "rejected_records": rejected_count,
        "rejection_rate_percent": round(rejection_rate, 2),
        "top_rejection_reasons": top_rejection_reasons
    }


def print_quality_report(metrics: Dict):
    print("\nDATA QUALITY REPORT")
    print("-------------------")
    print(f"Total records: {metrics['total_records']}")
    print(f"Clean records: {metrics['clean_records']}")
    print(f"Rejected records: {metrics['rejected_records']}")
    print(f"Rejection rate: {metrics['rejection_rate_percent']}%")

    print("\nTop rejection reasons:")
    for reason, count in metrics["top_rejection_reasons"]:
        print(f" - {reason}: {count}")
