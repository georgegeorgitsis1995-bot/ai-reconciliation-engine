import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from security.tokenize import payment_code_token

load_dotenv()

uri = os.getenv("MONGODB_URI")
db_name = os.getenv("MONGODB_DB", "ai_recon")
token_key = os.getenv("TOKEN_KEY")

if not uri or not token_key:
    raise RuntimeError("Missing MONGODB_URI / TOKEN_KEY in .env")

client = MongoClient(uri)
db = client[db_name]

scheme = db["scheme_transactions"]
bank = db["bank_transactions"]

runs = db["recon_runs"]
feedback = db["recon_feedback"]


def validate_date(date_str: str) -> str:
    # Expect YYYYMMDD
    datetime.strptime(date_str, "%Y%m%d")
    return date_str


def key(doc):
    return (doc["rf_token"], doc["date1"], doc["date2"], doc["date3"], doc.get("amount_int"))


def fetch_docs_for_date(col, date_str: str):
    # Using date1 as the primary filter (we can change later if you want date2/date3)
    proj = {"rf": 1, "rf_token": 1, "date1": 1, "date2": 1, "date3": 1, "amount_int": 1, "file_name": 1, "line_no": 1}
    return list(col.find({"date1": date_str}, proj))


def classify_unmatched_bank(doc):
    """
    Heuristic explanations (v1).
    We’ll evolve these using your feedback + learned rules.
    """
    rf = doc.get("rf", "") or ""
    amt = doc.get("amount_int")

    if not rf.startswith("RF"):
        return "RF_NOT_PRESENT_OR_DIFFERENT_FORMAT"

    if amt is None:
        return "AMOUNT_NOT_NUMERIC_OR_MISSING"

    # You can add bank-specific patterns here later (EPS IDs, fees, etc.)
    # For now: generic reasons.
    return "NOT_IN_SCHEME_FOR_DATE1"


def export_csv(path, rows, headers):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h) for h in headers})


def run_reconciliation(date_str: str):
    scheme_docs = fetch_docs_for_date(scheme, date_str)
    bank_docs = fetch_docs_for_date(bank, date_str)

    scheme_map = {key(d): d for d in scheme_docs}
    bank_map = {key(d): d for d in bank_docs}

    scheme_keys = set(scheme_map.keys())
    bank_keys = set(bank_map.keys())

    matched_keys = scheme_keys & bank_keys
    scheme_unmatched_keys = scheme_keys - bank_keys
    bank_unmatched_keys = bank_keys - scheme_keys

    matched = []
    for k in matched_keys:
        s = scheme_map[k]
        b = bank_map[k]
        matched.append({
            "rf": s.get("rf"),
            "amount_int": s.get("amount_int"),
            "date1": s.get("date1"),
            "date2": s.get("date2"),
            "date3": s.get("date3"),
            "scheme_file": s.get("file_name"),
            "scheme_line": s.get("line_no"),
            "bank_file": b.get("file_name"),
            "bank_line": b.get("line_no"),
        })

    scheme_unmatched = []
    for k in scheme_unmatched_keys:
        s = scheme_map[k]
        scheme_unmatched.append({
            "rf": s.get("rf"),
            "amount_int": s.get("amount_int"),
            "date1": s.get("date1"),
            "date2": s.get("date2"),
            "date3": s.get("date3"),
            "scheme_file": s.get("file_name"),
            "scheme_line": s.get("line_no"),
            "reason": "NOT_IN_BANK_FOR_DATE1"
        })

    bank_unmatched = []
    for k in bank_unmatched_keys:
        b = bank_map[k]
        reason = classify_unmatched_bank(b)
        bank_unmatched.append({
            "rf": b.get("rf"),
            "amount_int": b.get("amount_int"),
            "date1": b.get("date1"),
            "date2": b.get("date2"),
            "date3": b.get("date3"),
            "bank_file": b.get("file_name"),
            "bank_line": b.get("line_no"),
            "reason": reason
        })

    return scheme_docs, bank_docs, matched, scheme_unmatched, bank_unmatched


def suggest_improvements(bank_unmatched):
    """
    Simple v1 suggestions based on patterns.
    Later: upgrade this using your feedback + optional LLM.
    """
    reasons = {}
    for r in bank_unmatched:
        reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1

    suggestions = []
    # Example: if many are NOT_IN_SCHEME_FOR_DATE1, suggest checking date alignment (date2/date3)
    if reasons.get("NOT_IN_SCHEME_FOR_DATE1", 0) > 0:
        suggestions.append("Many bank records have no scheme match for date1. Consider trying reconciliation on date2 or date3, or widening date window (T+1/T+2).")

    if reasons.get("RF_NOT_PRESENT_OR_DIFFERENT_FORMAT", 0) > 0:
        suggestions.append("Some bank records do not have RF in the expected field. Consider extracting RF from another position or linking using an alternate ID (e.g., EPS/internal ID) as fallback.")

    if reasons.get("AMOUNT_NOT_NUMERIC_OR_MISSING", 0) > 0:
        suggestions.append("Some amounts are missing/non-numeric. Confirm amount slice positions and implied decimals, or parse amount as signed/packed if applicable.")

    if not suggestions:
        suggestions.append("No obvious improvements detected from current heuristics.")

    return suggestions


def store_run(date_str, counts, suggestions):
    doc = {
        "date1": date_str,
        "run_ts": datetime.utcnow().isoformat() + "Z",
        "counts": counts,
        "suggestions": suggestions,
    }
    runs.insert_one(doc)
    return doc


def ask_for_feedback():
    print("\nDo you want to label any unmatched cases to help the agent learn? (y/n)")
    ans = input("> ").strip().lower()
    return ans == "y"


def record_feedback(date_str, kind, rf, amount_int, label, note=""):
    feedback.insert_one({
        "date1": date_str,
        "kind": kind,  # "bank_unmatched" or "scheme_unmatched"
        "rf": rf,
        "amount_int": amount_int,
        "label": label,  # e.g. "FEE", "REVERSAL", "LATE_POSTING", "DATA_ISSUE", "EXPECTED"
        "note": note,
        "ts": datetime.utcnow().isoformat() + "Z"
    })


def main():
    print("AI Recon Agent")
    print("Enter date as YYYYMMDD (example: 20251107)")
    date_str = validate_date(input("> ").strip())

    scheme_docs, bank_docs, matched, scheme_unmatched, bank_unmatched = run_reconciliation(date_str)

    counts = {
        "scheme_records_for_date1": len(scheme_docs),
        "bank_records_for_date1": len(bank_docs),
        "matched": len(matched),
        "scheme_unmatched": len(scheme_unmatched),
        "bank_unmatched": len(bank_unmatched),
    }

    print("\n=== RECON RESULTS ===")
    for k, v in counts.items():
        print(f"{k}: {v}")

    # Export CSV reports
    os.makedirs("reports", exist_ok=True)
    matched_path = f"reports/matched_{date_str}.csv"
    scheme_unmatched_path = f"reports/scheme_unmatched_{date_str}.csv"
    bank_unmatched_path = f"reports/bank_unmatched_{date_str}.csv"

    export_csv(matched_path, matched, ["rf","amount_int","date1","date2","date3","scheme_file","scheme_line","bank_file","bank_line"])
    export_csv(scheme_unmatched_path, scheme_unmatched, ["rf","amount_int","date1","date2","date3","scheme_file","scheme_line","reason"])
    export_csv(bank_unmatched_path, bank_unmatched, ["rf","amount_int","date1","date2","date3","bank_file","bank_line","reason"])

    print("\nReports saved:")
    print(" -", matched_path)
    print(" -", scheme_unmatched_path)
    print(" -", bank_unmatched_path)

    # Explanations summary
    reason_counts = {}
    for r in bank_unmatched:
        reason_counts[r["reason"]] = reason_counts.get(r["reason"], 0) + 1

    print("\nTop bank-unmatched reasons:")
    for reason, cnt in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f" - {reason}: {cnt}")

    # Suggestions
    suggestions = suggest_improvements(bank_unmatched)
    print("\nSuggestions:")
    for s in suggestions:
        print(" -", s)

    store_run(date_str, counts, suggestions)

    # Learning loop (human feedback)
    if ask_for_feedback() and bank_unmatched:
        print("\nLabel a few bank-unmatched items (type 'stop' to finish).")
        print("Example labels: FEE, REVERSAL, LATE_POSTING, EXPECTED, DATA_ISSUE")
        for r in bank_unmatched[:20]:
            print(f"\nRF={r['rf']} amount={r['amount_int']} reason={r['reason']}")
            label = input("Label> ").strip()
            if label.lower() == "stop":
                break
            note = input("Note (optional)> ").strip()
            record_feedback(date_str, "bank_unmatched", r["rf"], r["amount_int"], label, note)

        print("\n✅ Feedback stored. Next runs can use these labels to improve explanations and rule suggestions.")

if __name__ == "__main__":
    main()
