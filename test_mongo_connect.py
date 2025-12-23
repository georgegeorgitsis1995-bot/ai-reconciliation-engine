import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

uri = os.getenv("MONGODB_URI")
db_name = os.getenv("MONGODB_DB", "ai_recon")

if not uri:
    raise RuntimeError("MONGODB_URI not found in .env")

client = MongoClient(uri)
client.admin.command("ping")

db = client[db_name]
col = db["healthcheck"]

col.insert_one({"status": "ok"})
doc = col.find_one({"status": "ok"})

print("✅ Connected to MongoDB Atlas")
print("✅ Write / Read OK:", doc["status"])
