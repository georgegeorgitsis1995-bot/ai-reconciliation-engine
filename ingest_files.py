import os
from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne
from security.tokenize import payment_code_token

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "ai_recon")
TOKEN_KEY = os.getenv("TOKEN_KEY")

if not MONGODB_URI or not TOKEN_KEY:
    raise RuntimeError("Missing MONGODB_URI or TOKEN_KEY in .env")

client = MongoClient(MONGODB_URI)
db = client[MONGODB_DB]

scheme_col = db["scheme_transactions"]
bank_col = db["bank_transactions"]

def s(line: str, start: int, end: int) -> str:
    """1-based inclusive slice (like column positions in specs)."""
    return line[start - 1:end]

def parse_detail(line: str, source: str) -> dict:
    """
    Parse only record type 11.
    source: 'scheme' or 'bank'
    """
    rec_type = s(line, 1, 2)
    if rec_type != "11":
        return {}

    amount_raw = s(line, 27, 38).strip()  # user-defined
    d1 = s(line, 55, 62).strip()
    d2 = s(line, 63, 70).strip()
    d3 = s(line, 71, 78).strip()

    if source == "scheme":
        rf = s(line, 183, 207).strip()
    elif source == "bank":
        rf = s(line, 101, 125).strip()
    else:
        raise ValueError("Unknown source")

    # Defensive normalization
    rf = rf.strip()
    amount_raw = amount_raw.strip()

    doc = {
        "record_type": rec_type,
        "amount_raw": amount_raw,
        "amount_int": int(amount_raw) if amount_raw.isdigit() else None,
        "date1": d1,
        "date2": d2,
        "date3": d3,
        "rf": rf,
        "rf_token": payment_code_token(rf, TOKEN_KEY),
    }
    return doc

def ingest_file(path: str, source: str, collection_name: str, batch_size: int = 5000):
    col = db[collection_name]

    ops = []
    total = 0
    inserted = 0

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f, start=1):
            line = line.rstrip("\n")
            total += 1

            doc = parse_detail(line, source)
            if not doc:
                continue

            # Traceability
            doc.update({
                "source": source,
                "file_name": os.path.basename(path),
                "line_no": i,
                "raw": line
            })

            ops.append(InsertOne(doc))

            if len(ops) >= batch_size:
                res = col.bulk_write(ops, ordered=False)
                inserted += res.inserted_count
                ops = []

    if ops:
        res = col.bulk_write(ops, ordered=False)
        inserted += res.inserted_count

    print(f"✅ Ingested {inserted} detail records into {collection_name} (from {total} lines)")

def ensure_indexes():
    # Compound indexes for fast reconciliation joins
    key = [("rf_token", 1), ("date1", 1), ("date2", 1), ("date3", 1), ("amount_int", 1)]
    scheme_col.create_index(key, name="idx_recon_key")
    bank_col.create_index(key, name="idx_recon_key")
    print("✅ Indexes ensured")

if __name__ == "__main__":
    # Update these paths to your local files
    scheme_path = r"C:\Users\georgig\AI_Recon\Files\Scheme\D0406"
    bank_path   = r"C:\Users\georgig\AI_Recon\Files\Bank\BN251106.001"

    # Optional: clear existing demo data
    scheme_col.delete_many({})
    bank_col.delete_many({})

    ensure_indexes()
    ingest_file(scheme_path, source="scheme", collection_name="scheme_transactions")
    ingest_file(bank_path, source="bank", collection_name="bank_transactions")
