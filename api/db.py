import os
from pymongo import MongoClient

def get_db():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGO_DB", "nosql_logs")
    client = MongoClient(uri)
    return client[db_name]
