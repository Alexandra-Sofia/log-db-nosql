import os
from faker import Faker
from pymongo import MongoClient

uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
db_name = os.getenv("MONGO_DB", "nosql_logs")
n = int(os.getenv("N_ADMINS", "500"))

fake = Faker()

client = MongoClient(uri)
db = client[db_name]
admins = db["admins"]

def main():
    docs = []
    for _ in range(n):
        username = fake.unique.user_name()
        docs.append({
            "username": username,
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "totalUpvotes": 0,
        })
    admins.insert_many(docs)
    print(f"Inserted {len(docs)} admins")

if __name__ == "__main__":
    main()
