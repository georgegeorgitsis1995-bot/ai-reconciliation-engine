import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

uri = os.getenv("MONGODB_URI")
db_name = os.getenv("MONGODB_DB", "ai_recon")

client = MongoClient(uri)
db = client[db_name]

db["scheme_transactions"].drop()
db["bank_transactions"].drop()

print("✅ Collections dropped")
