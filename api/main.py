from datetime import datetime, timezone
from typing import Any, Optional

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Query
from pymongo.errors import DuplicateKeyError

from db import get_db
from schemas import AdminCreate, LogCreate, UpvoteCreate

"""
NoSQL-LOGS API.

This FastAPI service exposes endpoints to:
    * insert logs
    * create administrators
    * cast upvotes
    * run analytics queries (Q1 to Q11)

MongoDB is used as the backing store and documents are converted into JSON-safe
representations for API responses.
"""

app = FastAPI(title="NoSQL-LOGS API")

db = get_db()
logs = db["logs"]
admins = db["admins"]
upvotes = db["upvotes"]
user_actions = db["user_actions"]


def ensure_utc(dt: datetime) -> datetime:
    """
    Normalize a datetime value to UTC.

    If a naive datetime is provided, it is interpreted as UTC.

    :param dt: Datetime value.
    :return: datetime
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def day_str(dt: datetime) -> str:
    """
    Convert a datetime value to an ISO-formatted UTC day string.

    Format example:
        2026-01-23

    :param dt: Datetime value.
    :return: str
    """
    return ensure_utc(dt).date().isoformat()


def oid(value: str) -> ObjectId:
    """
    Parse a string into a MongoDB ObjectId.

    :param value: ObjectId string.
    :return: ObjectId
    """
    try:
        return ObjectId(value)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ObjectId")


def to_jsonable(value: Any) -> Any:
    """
    Convert a MongoDB object into JSON-serializable form.

    This converts:
        * ObjectId -> str
        * datetime -> ISO-8601 str
        * lists and dicts recursively

    :param value: Arbitrary value.
    :return: Any
    """
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    return value


@app.get("/health")
def health() -> dict[str, bool]:
    """
    Healthcheck endpoint.

    :param None: This function does not accept any parameters.
    :return: dict[str, bool]
    """
    return {"ok": True}


@app.post("/insert-logs")
def insert_log(payload: LogCreate) -> dict[str, str]:
    """
    Insert a single log document.

    The endpoint normalizes timestamps to UTC, derives the day string, and
    initializes upvoteCount to 0.

    For ACCESS logs, it validates that the access subdocument exists and
    derives actionType from access.method.

    :param payload: LogCreate
    :return: dict[str, str]
    """
    doc = payload.model_dump()

    ts = ensure_utc(payload.ts)
    doc["ts"] = ts
    doc["day"] = day_str(ts)
    doc["upvoteCount"] = 0

    if doc.get("logSet") == "ACCESS":
        if payload.access is None:
            raise HTTPException(status_code=400, detail="ACCESS log requires access fields")
        if payload.sourceIp is None:
            raise HTTPException(status_code=400, detail="ACCESS log requires sourceIp")
        doc["access"] = payload.access.model_dump()
        doc["actionType"] = payload.access.method

    res = logs.insert_one(doc)
    return {"id": str(res.inserted_id)}


@app.post("/create-admin")
def create_admin(payload: AdminCreate) -> dict[str, str]:
    """
    Create a single administrator document.

    totalUpvotes is initialized to 0.

    :param payload: AdminCreate
    :return: dict[str, str]
    """
    doc = payload.model_dump()
    doc["totalUpvotes"] = 0
    res = admins.insert_one(doc)
    return {"id": str(res.inserted_id)}


@app.post("/cast-upvote")
def cast_upvote(payload: UpvoteCreate) -> dict[str, bool]:
    """
    Cast an upvote by an administrator on a log.

    This endpoint:
        * validates admin and log existence
        * enforces uniqueness via the upvotes unique index
        * stores denormalized admin/log fields for analytics convenience
        * increments upvoteCount and totalUpvotes counters

    :param payload: UpvoteCreate
    :return: dict[str, bool]
    """
    admin_id = oid(payload.adminId)
    log_id = oid(payload.logId)

    admin = admins.find_one({"_id": admin_id})
    if not admin:
        raise HTTPException(status_code=404, detail="Admin not found")

    log_doc = logs.find_one({"_id": log_id})
    if not log_doc:
        raise HTTPException(status_code=404, detail="Log not found")

    block_ids: list[int] = []
    if isinstance(log_doc.get("blockId"), int):
        block_ids.append(log_doc["blockId"])

    vote_doc = {
        "adminId": admin_id,
        "logId": log_id,
        "ts": datetime.now(timezone.utc),
        "day": log_doc.get("day"),
        "emailUsed": admin.get("email"),
        "usernameUsed": admin.get("username"),
        "sourceIp": log_doc.get("sourceIp"),
        "blockIds": block_ids,
    }

    try:
        upvotes.insert_one(vote_doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Duplicate vote for same admin and log")

    logs.update_one({"_id": log_id}, {"$inc": {"upvoteCount": 1}})
    admins.update_one({"_id": admin_id}, {"$inc": {"totalUpvotes": 1}})

    return {"ok": True}


@app.get("/debug/count")
def debug_count() -> dict[str, int]:
    """
    Debug endpoint returning total log count.

    :param None: This function does not accept any parameters.
    :return: dict[str, int]
    """
    return {"logs": logs.count_documents({})}


@app.get("/analytics/logs-per-type")
def q1_logs_per_type(
    start: datetime,
    end: datetime,
    logSet: Optional[str] = None,
) -> dict[str, Any]:
    """
    Q1: Count logs per action type within a time range, optionally filtered by logSet.

    :param start: Start datetime (inclusive).
    :param end: End datetime (inclusive).
    :param logSet: Optional logSet filter.
    :return: dict[str, Any]
    """
    start = ensure_utc(start)
    end = ensure_utc(end)

    match = {"ts": {"$gte": start, "$lte": end}}
    if logSet:
        match["logSet"] = logSet

    pipeline = [
        {"$match": match},
        {"$group": {"_id": "$actionType", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
    ]
    return {"results": to_jsonable(list(logs.aggregate(pipeline)))}


@app.get("/analytics/requests-per-day")
def q2_requests_per_day(
    logSet: str,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    """
    Q2: Count requests per day for a specific logSet within a time range.

    :param logSet: Log set name.
    :param start: Start datetime (inclusive).
    :param end: End datetime (inclusive).
    :return: dict[str, Any]
    """
    start = ensure_utc(start)
    end = ensure_utc(end)

    pipeline = [
        {"$match": {"logSet": logSet, "ts": {"$gte": start, "$lte": end}}},
        {"$group": {"_id": "$day", "total": {"$sum": 1}}},
        {"$sort": {"_id": -1}},
    ]
    return {"results": to_jsonable(list(logs.aggregate(pipeline)))}


@app.get("/analytics/top3-per-sourceip")
def q3_top3_per_sourceip(day: str = Query(..., description="YYYY-MM-DD")) -> dict[str, Any]:
    """
    Q3: For each sourceIp on a given day, return the top 3 most common request signatures.

    For ACCESS logs the signature is:
        "<method> <resource>"

    For other log sets the signature falls back to:
        "<actionType>"

    :param day: Day string in YYYY-MM-DD.
    :return: dict[str, Any]
    """
    pipeline = [
        {"$match": {"day": day, "sourceIp": {"$ne": None}}},
        {"$addFields": {
            "sig": {
                "$cond": [
                    {"$eq": ["$logSet", "ACCESS"]},
                    {
                        "$concat": [
                            {"$ifNull": ["$access.method", ""]},
                            " ",
                            {"$ifNull": ["$access.resource", ""]},
                        ]
                    },
                    {"$ifNull": ["$actionType", "UNKNOWN"]},
                ]
            }
        }},
        {"$group": {"_id": {"sourceIp": "$sourceIp", "sig": "$sig"}, "cnt": {"$sum": 1}}},
        {"$sort": {"_id.sourceIp": 1, "cnt": -1, "_id.sig": 1}},
        {"$group": {"_id": "$_id.sourceIp", "top": {"$push": {"sig": "$_id.sig", "cnt": "$cnt"}}}},
        {"$project": {"_id": 0, "sourceIp": "$_id", "top": {"$slice": ["$top", 3]}}},
    ]
    return {"results": to_jsonable(list(logs.aggregate(pipeline)))}


@app.get("/analytics/least-http-methods")
def q4_least_http_methods(start: datetime, end: datetime) -> dict[str, Any]:
    """
    Q4: Return the two least used HTTP methods in ACCESS logs within a time range.

    :param start: Start datetime (inclusive).
    :param end: End datetime (inclusive).
    :return: dict[str, Any]
    """
    start = ensure_utc(start)
    end = ensure_utc(end)

    pipeline = [
        {"$match": {"logSet": "ACCESS", "ts": {"$gte": start, "$lte": end}}},
        {"$group": {"_id": "$access.method", "cnt": {"$sum": 1}}},
        {"$sort": {"cnt": 1}},
        {"$limit": 2},
    ]
    return {"results": to_jsonable(list(logs.aggregate(pipeline)))}


@app.get("/analytics/referrers-multi-resource")
def q5_referrers_multi_resource() -> dict[str, Any]:
    """
    Q5: Find referrers that point to more than one distinct resource.

    :param None: This function does not accept any parameters.
    :return: dict[str, Any]
    """
    pipeline = [
        {"$match": {"logSet": "ACCESS", "access.referrer": {"$nin": [None, "-", ""]}}},
        {"$group": {"_id": "$access.referrer", "resources": {"$addToSet": "$access.resource"}}},
        {"$project": {"resources": 1, "resourceCount": {"$size": "$resources"}}},
        {"$match": {"resourceCount": {"$gt": 1}}},
        {"$sort": {"resourceCount": -1}},
    ]
    return {"results": to_jsonable(list(logs.aggregate(pipeline)))}


@app.get("/analytics/blocks-replicated-and-served")
def q6_blocks_replicated_and_served(day: str = Query(..., description="YYYY-MM-DD")) -> dict[str, Any]:
    """
    Q6: Return blocks that were both replicated and served on a given day.

    :param day: Day string in YYYY-MM-DD.
    :return: dict[str, Any]
    """
    pipeline = [
        {"$match": {
            "day": day,
            "logSet": {"$in": ["HDFS_NAMESYSTEM", "HDFS_DATAXCEIVER"]},
            "actionType": {"$in": ["replicate", "served"]},
            "blockId": {"$ne": None},
        }},
        {"$group": {
            "_id": {"day": "$day", "blockId": "$blockId"},
            "hasReplicate": {"$max": {"$cond": [{"$eq": ["$actionType", "replicate"]}, 1, 0]}},
            "hasServed": {"$max": {"$cond": [{"$eq": ["$actionType", "served"]}, 1, 0]}},
        }},
        {"$match": {"hasReplicate": 1, "hasServed": 1}},
        {"$project": {"_id": 0, "day": "$_id.day", "blockId": "$_id.blockId"}},
    ]
    return {"results": to_jsonable(list(logs.aggregate(pipeline)))}


@app.get("/analytics/top-upvoted-logs")
def q7_top_upvoted_logs(day: str = Query(..., description="YYYY-MM-DD")) -> dict[str, Any]:
    """
    Q7: Return the top 50 upvoted logs for a given day.

    :param day: Day string in YYYY-MM-DD.
    :return: dict[str, Any]
    """
    cursor = logs.find({"day": day}).sort("upvoteCount", -1).limit(50)
    return {"results": to_jsonable(list(cursor))}


@app.get("/analytics/top-admins-upvotes")
def q8_top_admins_upvotes() -> dict[str, Any]:
    """
    Q8: Return the top 50 administrators by totalUpvotes.

    :param None: This function does not accept any parameters.
    :return: dict[str, Any]
    """
    cursor = admins.find(
        {},
        {"username": 1, "email": 1, "phone": 1, "totalUpvotes": 1},
    ).sort("totalUpvotes", -1).limit(50)
    return {"results": to_jsonable(list(cursor))}


@app.get("/analytics/top-admins-sourceips")
def q9_top_admins_sourceips() -> dict[str, Any]:
    """
    Q9: Return the top 50 administrators by number of distinct sourceIp values they voted on.

    :param None: This function does not accept any parameters.
    :return: dict[str, Any]
    """
    pipeline = [
        {"$match": {"sourceIp": {"$ne": None}}},
        {"$group": {"_id": "$adminId", "ips": {"$addToSet": "$sourceIp"}}},
        {"$project": {"ipCount": {"$size": "$ips"}}},
        {"$sort": {"ipCount": -1}},
        {"$limit": 50},
        {"$lookup": {"from": "admins", "localField": "_id", "foreignField": "_id", "as": "admin"}},
        {"$unwind": "$admin"},
        {"$project": {"adminId": {"$toString": "$_id"}, "username": "$admin.username", "email": "$admin.email", "ipCount": 1}},
    ]
    return {"results": to_jsonable(list(upvotes.aggregate(pipeline)))}


@app.get("/analytics/logs-multi-username-per-email")
def q10_logs_multi_username_per_email() -> dict[str, Any]:
    """
    Q10: Return logs that received upvotes from the same email using multiple usernames.

    :param None: This function does not accept any parameters.
    :return: dict[str, Any]
    """
    pipeline = [
        {"$group": {"_id": "$emailUsed", "usernames": {"$addToSet": "$usernameUsed"}, "logIds": {"$addToSet": "$logId"}}},
        {"$project": {"usernameCount": {"$size": "$usernames"}, "usernames": 1, "logIds": 1}},
        {"$match": {"usernameCount": {"$gt": 1}}},
        {"$unwind": "$logIds"},
        {"$group": {"_id": "$logIds", "emails": {"$addToSet": "$_id"}}},
        {"$lookup": {"from": "logs", "localField": "_id", "foreignField": "_id", "as": "log"}},
        {"$unwind": "$log"},
        {"$project": {"logId": {"$toString": "$_id"}, "emails": 1, "log": "$log"}},
    ]
    return {"results": to_jsonable(list(upvotes.aggregate(pipeline)))}


@app.get("/analytics/blockids-voted")
def q11_blockids_voted(username: str) -> dict[str, Any]:
    """
    Q11: Return all unique blockIds that were voted on by a specific username.

    :param username: Username used in upvotes.
    :return: dict[str, Any]
    """
    pipeline = [
        {"$match": {"usernameUsed": username}},
        {"$unwind": "$blockIds"},
        {"$group": {"_id": "$blockIds"}},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "blockId": "$_id"}},
    ]
    return {"results": to_jsonable(list(upvotes.aggregate(pipeline)))}
