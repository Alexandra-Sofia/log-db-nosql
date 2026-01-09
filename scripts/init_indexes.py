import os
from pymongo import MongoClient, ASCENDING, DESCENDING

uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
db_name = os.getenv("MONGO_DB", "nosql_logs")

client = MongoClient(uri)
db = client[db_name]

logs = db["logs"]
admins = db["admins"]
upvotes = db["upvotes"]

def main():
    logs.create_index([("ts", ASCENDING), ("logSet", ASCENDING), ("actionType", ASCENDING)])
    logs.create_index([("day", ASCENDING), ("logSet", ASCENDING)])
    logs.create_index([("day", ASCENDING), ("sourceIp", ASCENDING), ("logSet", ASCENDING)])
    logs.create_index([("day", ASCENDING), ("blockId", ASCENDING), ("actionType", ASCENDING), ("logSet", ASCENDING)])
    logs.create_index([("access.referrer", ASCENDING), ("access.resource", ASCENDING), ("logSet", ASCENDING)])
    logs.create_index([("day", ASCENDING), ("upvoteCount", DESCENDING)])

    upvotes.create_index([("adminId", ASCENDING), ("logId", ASCENDING)], unique=True)
    upvotes.create_index([("adminId", ASCENDING), ("day", ASCENDING)])
    upvotes.create_index([("emailUsed", ASCENDING), ("usernameUsed", ASCENDING)])
    upvotes.create_index([("adminId", ASCENDING), ("blockIds", ASCENDING)])

    admins.create_index([("totalUpvotes", DESCENDING)])
    admins.create_index([("username", ASCENDING)], unique=True)

    print("Indexes created")

if __name__ == "__main__":
    main()
