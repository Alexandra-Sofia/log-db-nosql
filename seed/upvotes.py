import os
import math
import random
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, DuplicateKeyError

from util import tiny_logger

URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "nosql_logs")

EXTRA_VOTE_FRAC = float(os.getenv("EXTRA_VOTE_FRAC", "0.20"))
ADMIN_CAP = int(os.getenv("ADMIN_CAP", "1000"))
SEED = int(os.getenv("SEED", "42"))

# If upvotes already exist, either stop or top-up.
# "fail" is safer for grading reproducibility.
MODE = os.getenv("MODE", "fail")  # "fail" | "topup"

client = MongoClient(URI)
db = client[DB_NAME]
logs = db["logs"]
admins = db["admins"]
upvotes = db["upvotes"]


def ensure_indexes():
    upvotes.create_index([("adminId", 1), ("logId", 1)], unique=True, name="uniq_admin_log_vote")
    admins.create_index([("username", 1)], unique=True, name="uniq_username")
    admins.create_index([("email", 1)], unique=True, name="uniq_email")
    logs.create_index([("day", 1), ("upvoteCount", -1)], name="day_upvotes_desc")
    logs.create_index([("upvoteCount", 1)], name="upvoteCount")  # helps find logs with 0 votes


def recompute_counters():
    tiny_logger("[SEED][UPVOTES] Recomputing counters from upvotes collection...")

    # logs.upvoteCount
    bulk_log_updates = []
    for row in upvotes.aggregate([{"$group": {"_id": "$logId", "cnt": {"$sum": 1}}}]):
        bulk_log_updates.append(UpdateOne({"_id": row["_id"]}, {"$set": {"upvoteCount": row["cnt"]}}))
    if bulk_log_updates:
        res = logs.bulk_write(bulk_log_updates, ordered=False)
        tiny_logger(f"[SEED][UPVOTES] Updated logs.upvoteCount for {res.modified_count} logs.")
    else:
        tiny_logger("[SEED][UPVOTES] No log counters to update (no upvotes yet).")

    # admins.totalUpvotes
    bulk_admin_updates = []
    for row in upvotes.aggregate([{"$group": {"_id": "$adminId", "cnt": {"$sum": 1}}}]):
        bulk_admin_updates.append(UpdateOne({"_id": row["_id"]}, {"$set": {"totalUpvotes": row["cnt"]}}))
    if bulk_admin_updates:
        res = admins.bulk_write(bulk_admin_updates, ordered=False)
        tiny_logger(f"[SEED][UPVOTES] Updated admins.totalUpvotes for {res.modified_count} admins.")
    else:
        tiny_logger("[SEED][UPVOTES] No admin counters to update (no upvotes yet).")


def main():
    tiny_logger("[SEED][UPVOTES] START")

    random.seed(SEED)
    ensure_indexes()

    L = logs.count_documents({})
    A = admins.count_documents({})
    U = upvotes.count_documents({})

    if L == 0:
        raise SystemExit("No logs found. Run ingestion first.")
    if A == 0:
        raise SystemExit("No admins found. Run admins.py first.")

    if U > 0 and MODE == "fail":
        raise SystemExit("Upvotes already exist. Set MODE=topup or clear the upvotes collection.")

    # Always start from true counters
    recompute_counters()

    target_covered = math.ceil(L / 3)

    # Coverage is computed from logs.upvoteCount
    covered_now = logs.count_documents({"upvoteCount": {"$gte": 1}})
    needed = max(0, target_covered - covered_now)

    # Determine total desired votes (optional richness)
    target_total_votes = target_covered + math.ceil(EXTRA_VOTE_FRAC * L)

    tiny_logger(f"[SEED][UPVOTES] Logs: {L}")
    tiny_logger(f"[SEED][UPVOTES] Admins: {A}")
    tiny_logger(f"[SEED][UPVOTES] Existing upvotes: {U}")
    tiny_logger(f"[SEED][UPVOTES] Covered logs now: {covered_now} (need at least {target_covered})")
    tiny_logger(f"[SEED][UPVOTES] Need additional covered logs: {needed}")
    tiny_logger(f"[SEED][UPVOTES] Target total votes: {target_total_votes} (EXTRA_VOTE_FRAC={EXTRA_VOTE_FRAC})")

    # Load admins and their current counts (enforce cap)
    admin_docs = list(admins.find({}, {"_id": 1, "email": 1, "username": 1, "totalUpvotes": 1}))
    admin_ids = [d["_id"] for d in admin_docs]
    admin_by_id = {d["_id"]: d for d in admin_docs}
    admin_vote_count = {d["_id"]: int(d.get("totalUpvotes", 0) or 0) for d in admin_docs}

    def pick_admin():
        for _ in range(100):
            aid = random.choice(admin_ids)
            if admin_vote_count[aid] < ADMIN_CAP:
                return aid
        return None

    now = datetime.now(timezone.utc)

    def make_vote(aid, log_doc):
        block_ids = []
        if isinstance(log_doc.get("blockId"), int):
            block_ids.append(log_doc["blockId"])
        a = admin_by_id[aid]
        return {
            "adminId": aid,
            "logId": log_doc["_id"],
            "ts": now,
            "day": log_doc.get("day"),
            "emailUsed": a.get("email"),
            "usernameUsed": a.get("username"),
            "sourceIp": log_doc.get("sourceIp"),
            "blockIds": block_ids,
        }

    inserted_votes = 0

    # 1) Guarantee coverage by selecting logs with upvoteCount == 0
    if needed > 0:
        tiny_logger(f"[SEED][UPVOTES] Ensuring coverage: inserting at least {needed} new votes on 0-vote logs...")
        cursor = logs.find(
            {"upvoteCount": 0},
            {"_id": 1, "day": 1, "sourceIp": 1, "blockId": 1},
        ).limit(needed)

        coverage_done = 0
        for log_doc in cursor:
            aid = pick_admin()
            if aid is None:
                raise SystemExit("Cannot allocate more votes without exceeding admin cap.")

            try:
                upvotes.insert_one(make_vote(aid, log_doc))
                inserted_votes += 1
                coverage_done += 1
                admin_vote_count[aid] += 1
                logs.update_one({"_id": log_doc["_id"]}, {"$inc": {"upvoteCount": 1}})
                admins.update_one({"_id": aid}, {"$inc": {"totalUpvotes": 1}})
            except DuplicateKeyError:
                # extremely unlikely here, but if it happens, retry once with another admin
                aid2 = pick_admin()
                if aid2 is None:
                    raise SystemExit("Cannot allocate more votes without exceeding admin cap.")
                upvotes.insert_one(make_vote(aid2, log_doc))
                inserted_votes += 1
                coverage_done += 1
                admin_vote_count[aid2] += 1
                logs.update_one({"_id": log_doc["_id"]}, {"$inc": {"upvoteCount": 1}})
                admins.update_one({"_id": aid2}, {"$inc": {"totalUpvotes": 1}})

            if coverage_done % 1000 == 0:
                tiny_logger(f"[SEED][UPVOTES] Coverage progress: {coverage_done}/{needed}")

        tiny_logger(f"[SEED][UPVOTES] Coverage step complete: inserted {coverage_done} votes.")
    else:
        tiny_logger("[SEED][UPVOTES] Coverage already satisfied; no mandatory votes needed.")

    # 2) Add extra votes for richer distributions (optional)
    current_votes = upvotes.count_documents({})
    remaining = max(0, target_total_votes - current_votes)

    if remaining > 0:
        tiny_logger(f"[SEED][UPVOTES] Adding extra votes for richness: remaining={remaining}")

        pool_size = min(L, max(20000, remaining * 3))
        tiny_logger(f"[SEED][UPVOTES] Building sampling pool of logs (pool_size={pool_size})...")
        sample_logs = list(logs.find({}, {"_id": 1, "day": 1, "sourceIp": 1, "blockId": 1}).limit(pool_size))

        batch = []
        batched = 0
        for _ in range(remaining):
            aid = pick_admin()
            if aid is None:
                tiny_logger("[SEED][UPVOTES] Reached admin cap across pool; stopping extra vote generation.")
                break

            log_doc = random.choice(sample_logs)
            batch.append(make_vote(aid, log_doc))
            admin_vote_count[aid] += 1

            if len(batch) >= 5000:
                try:
                    res = upvotes.insert_many(batch, ordered=False)
                    inserted_votes += len(res.inserted_ids)
                    batched += len(res.inserted_ids)
                except BulkWriteError as e:
                    inserted_votes += e.details.get("nInserted", 0)
                    batched += e.details.get("nInserted", 0)
                batch = []
                if batched % 20000 == 0:
                    tiny_logger(f"[SEED][UPVOTES] Extra votes inserted so far: {batched}")

        if batch:
            try:
                res = upvotes.insert_many(batch, ordered=False)
                inserted_votes += len(res.inserted_ids)
            except BulkWriteError as e:
                inserted_votes += e.details.get("nInserted", 0)

        tiny_logger(f"[SEED][UPVOTES] Extra vote step complete.")
    else:
        tiny_logger("[SEED][UPVOTES] No extra votes needed (already at/above target_total_votes).")

    # Final recompute for correctness and validation
    recompute_counters()

    covered = logs.count_documents({"upvoteCount": {"$gte": 1}})
    max_admin = admins.find({}, {"totalUpvotes": 1}).sort("totalUpvotes", -1).limit(1)
    max_admin_val = next(max_admin, {}).get("totalUpvotes", 0)

    tiny_logger(f"[SEED][UPVOTES] Inserted votes this run: {inserted_votes}")
    tiny_logger(f"[SEED][UPVOTES] Logs with >=1 upvote: {covered} (required >= {target_covered})")
    tiny_logger(f"[SEED][UPVOTES] Max admin totalUpvotes: {max_admin_val} (cap {ADMIN_CAP})")

    if covered < target_covered:
        raise SystemExit("Constraint failed: fewer than 1/3 logs have upvotes.")
    if max_admin_val > ADMIN_CAP:
        raise SystemExit("Constraint failed: an admin exceeded 1000 upvotes.")

    tiny_logger("[SEED][UPVOTES] END")


if __name__ == "__main__":
    main()
