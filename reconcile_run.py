import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

uri = os.getenv("MONGODB_URI")
db_name = os.getenv("MONGODB_DB", "ai_recon")

client = MongoClient(uri)
db = client[db_name]

scheme = db["scheme_transactions"]
bank = db["bank_transactions"]

def recon_key(doc):
    return (doc["rf_token"], doc["date1"], doc["date2"], doc["date3"], doc.get("amount_int"))

# Pull only what we need
scheme_docs = list(scheme.find({}, {"rf_token": 1, "date1": 1, "date2": 1, "date3": 1, "amount_int": 1, "rf": 1}))
bank_docs   = list(bank.find({},   {"rf_token": 1, "date1": 1, "date2": 1, "date3": 1, "amount_int": 1, "rf": 1}))

scheme_set = {recon_key(d) for d in scheme_docs}
bank_set   = {recon_key(d) for d in bank_docs}

matched = scheme_set & bank_set
scheme_unmatched = scheme_set - bank_set
bank_unmatched = bank_set - scheme_set

print("\n=== RECON RESULTS ===")
print("Scheme records:", len(scheme_set))
print("Bank records:  ", len(bank_set))
print("Matched:       ", len(matched))
print("Unmatched Scheme:", len(scheme_unmatched))
print("Unmatched Bank:  ", len(bank_unmatched))

# Show a few examples (RF is safe-ish here because it's your test data)
def show_examples(label, keyset, docs, limit=5):
    print(f"\n{label} (showing up to {limit}):")
    shown = 0
    keys = set(list(keyset)[:limit])
    for d in docs:
        if recon_key(d) in keys:
            print("  RF:", d.get("rf"), "amount:", d.get("amount_int"), "dates:", d["date1"], d["date2"], d["date3"])
            shown += 1
            if shown >= limit:
                break

show_examples("Matched", matched, scheme_docs)
show_examples("Unmatched in Scheme", scheme_unmatched, scheme_docs)
show_examples("Unmatched in Bank", bank_unmatched, bank_docs)
