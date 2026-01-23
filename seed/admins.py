import os
from faker import Faker
from pymongo import MongoClient

from util import tiny_logger

uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
db_name = os.getenv("MONGO_DB", "nosql_logs")
n = int(os.getenv("N_ADMINS", "500"))

fake = Faker()

client = MongoClient(uri)
db = client[db_name]
admins = db["admins"]


def main():
    existing = admins.count_documents({})
    if existing > 0:
        tiny_logger(f"[SEED][ADMINS] Admins already exist ({existing}). Skipping.")
        return

    tiny_logger(f"[SEED][ADMINS] Generating {n} administrators...")

    docs = []
    for _ in range(n):
        username = fake.unique.user_name()
        docs.append({
            "username": username,
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "totalUpvotes": 0,
        })

    result = admins.insert_many(docs)
    tiny_logger(f"[SEED][ADMINS] Inserted {len(result.inserted_ids)} admins.")


if __name__ == "__main__":
    main()
