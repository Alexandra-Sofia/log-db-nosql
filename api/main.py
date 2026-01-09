from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from db import get_db
from schemas import LogCreate, UpvoteCreate, AdminCreate

app = FastAPI(title="NoSQL-LOGS API")

db = get_db()
logs = db["logs"]
admins = db["admins"]
upvotes = db["upvotes"]
user_actions = db["user_actions"]

def day_str(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.date().isoformat()

def oid(s: str) -> ObjectId:
    try:
        return ObjectId(s)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ObjectId")

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/logs")
def insert_log(payload: LogCreate):
    doc = payload.model_dump()
    doc["day"] = day_str(payload.ts)
    doc["upvoteCount"] = 0

    if payload.logSet == "ACCESS":
        if not payload.access:
            raise HTTPException(status_code=400, detail="ACCESS log requires access fields")
        if not payload.sourceIp:
            raise HTTPException(status_code=400, detail="ACCESS log requires sourceIp")
        doc["access"]["method"] = payload.access.method
        doc["actionType"] = payload.access.method

    res = logs.insert_one(doc)
    return {"id": str(res.inserted_id)}

@app.post("/admins")
def create_admin(payload: AdminCreate):
    doc = payload.model_dump()
    doc["totalUpvotes"] = 0
    res = admins.insert_one(doc)
    return {"id": str(res.inserted_id)}

@app.post("/upvotes")
def cast_upvote(payload: UpvoteCreate):
    admin_id = oid(payload.adminId)
    log_id = oid(payload.logId)

    admin = admins.find_one({"_id": admin_id})
    if not admin:
        raise HTTPException(status_code=404, detail="Admin not found")

    log_doc = logs.find_one({"_id": log_id})
    if not log_doc:
        raise HTTPException(status_code=404, detail="Log not found")

    vote_doc = {
        "adminId": admin_id,
        "logId": log_id,
        "ts": datetime.now(timezone.utc),
        "day": log_doc.get("day"),
        "emailUsed": admin.get("email"),
        "usernameUsed": admin.get("username"),
        "sourceIp": log_doc.get("sourceIp"),
        "blockIds": [],
    }

    if log_doc.get("blockId") is not None:
        vote_doc["blockIds"].append(log_doc["blockId"])
    if log_doc.get("hdfs") and isinstance(log_doc["hdfs"].get("blockIds"), list):
        vote_doc["blockIds"].extend([b for b in log_doc["hdfs"]["blockIds"] if isinstance(b, int)])

    try:
        upvotes.insert_one(vote_doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Duplicate vote for same admin and log")

    logs.update_one({"_id": log_id}, {"$inc": {"upvoteCount": 1}})
    admins.update_one({"_id": admin_id}, {"$inc": {"totalUpvotes": 1}})

    return {"ok": True}

# Q1
@app.get("/analytics/logs-per-type")
def q1_logs_per_type(
    start: datetime,
    end: datetime,
    logSet: Optional[str] = None,
):
    match = {"ts": {"$gte": start, "$lte": end}}
    if logSet:
        match["logSet"] = logSet

    pipeline = [
        {"$match": match},
        {"$group": {"_id": "$actionType", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
    ]
    return {"results": list(logs.aggregate(pipeline))}

# Q2
@app.get("/analytics/requests-per-day")
def q2_requests_per_day(
    logSet: str,
    start: datetime,
    end: datetime,
):
    pipeline = [
        {"$match": {"logSet": logSet, "ts": {"$gte": start, "$lte": end}}},
        {"$group": {"_id": "$day", "total": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    return {"results": list(logs.aggregate(pipeline))}

# Q3 (ACCESS version: top 3 resources per sourceIp for a day)
@app.get("/analytics/top3-per-sourceip")
def q3_top3_per_sourceip(
    day: str = Query(..., description="YYYY-MM-DD"),
    logSet: str = Query("ACCESS"),
):
    if logSet != "ACCESS":
        pipeline = [
            {"$match": {"day": day, "logSet": logSet}},
            {"$group": {"_id": {"sourceIp": "$sourceIp", "actionType": "$actionType"}, "cnt": {"$sum": 1}}},
            {"$sort": {"_id.sourceIp": 1, "cnt": -1}},
            {"$group": {"_id": "$_id.sourceIp", "top": {"$push": {"actionType": "$_id.actionType", "cnt": "$cnt"}}}},
            {"$project": {"top": {"$slice": ["$top", 3]}}},
        ]
        return {"results": list(logs.aggregate(pipeline))}

    pipeline = [
        {"$match": {"day": day, "logSet": "ACCESS"}},
        {"$group": {"_id": {"sourceIp": "$sourceIp", "resource": "$access.resource"}, "cnt": {"$sum": 1}}},
        {"$sort": {"_id.sourceIp": 1, "cnt": -1}},
        {"$group": {"_id": "$_id.sourceIp", "top": {"$push": {"resource": "$_id.resource", "cnt": "$cnt"}}}},
        {"$project": {"top": {"$slice": ["$top", 3]}}},
    ]
    return {"results": list(logs.aggregate(pipeline))}

# Q4
@app.get("/analytics/least-http-methods")
def q4_least_http_methods(start: datetime, end: datetime):
    pipeline = [
        {"$match": {"logSet": "ACCESS", "ts": {"$gte": start, "$lte": end}}},
        {"$group": {"_id": "$access.method", "cnt": {"$sum": 1}}},
        {"$sort": {"cnt": 1}},
        {"$limit": 2},
    ]
    return {"results": list(logs.aggregate(pipeline))}

# Q5
@app.get("/analytics/referrers-multi-resource")
def q5_referrers_multi_resource():
    pipeline = [
        {"$match": {"logSet": "ACCESS", "access.referrer": {"$nin": [None, "-", ""]}}},
        {"$group": {"_id": "$access.referrer", "resources": {"$addToSet": "$access.resource"}}},
        {"$project": {"resources": 1, "resourceCount": {"$size": "$resources"}}},
        {"$match": {"resourceCount": {"$gt": 1}}},
        {"$sort": {"resourceCount": -1}},
    ]
    return {"results": list(logs.aggregate(pipeline))}

# Q6
@app.get("/analytics/blocks-replicated-and-served")
def q6_blocks_replicated_and_served(day: str = Query(..., description="YYYY-MM-DD")):
    pipeline = [
        {
            "$match": {
                "day": day,
                "logSet": {"$in": ["HDFS_NAMESYSTEM", "HDFS_DATAXCEIVER"]},
                "actionType": {"$in": ["replicate", "served"]},
                "blockId": {"$ne": None},
            }
        },
        {
            "$group": {
                "_id": {"day": "$day", "blockId": "$blockId"},
                "hasReplicate": {"$max": {"$cond": [{"$eq": ["$actionType", "replicate"]}, 1, 0]}},
                "hasServed": {"$max": {"$cond": [{"$eq": ["$actionType", "served"]}, 1, 0]}},
            }
        },
        {"$match": {"hasReplicate": 1, "hasServed": 1}},
        {"$project": {"_id": 0, "day": "$_id.day", "blockId": "$_id.blockId"}},
    ]
    return {"results": list(logs.aggregate(pipeline))}

# Q7
@app.get("/analytics/top-upvoted-logs")
def q7_top_upvoted_logs(day: str = Query(..., description="YYYY-MM-DD")):
    cursor = logs.find({"day": day}).sort("upvoteCount", -1).limit(50)
    return {"results": [serialize_log(x) for x in cursor]}

# Q8
@app.get("/analytics/top-admins-upvotes")
def q8_top_admins_upvotes():
    cursor = admins.find({}, {"username": 1, "email": 1, "phone": 1, "totalUpvotes": 1}).sort("totalUpvotes", -1).limit(50)
    return {"results": [serialize_admin(x) for x in cursor]}

# Q9
@app.get("/analytics/top-admins-sourceips")
def q9_top_admins_sourceips():
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
    return {"results": list(upvotes.aggregate(pipeline))}

# Q10
@app.get("/analytics/logs-multi-username-per-email")
def q10_logs_multi_username_per_email():
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
    results = list(upvotes.aggregate(pipeline))
    for r in results:
        r["log"] = serialize_log(r["log"])
    return {"results": results}

# Q11
@app.get("/analytics/blockids-voted")
def q11_blockids_voted(adminId: str):
    admin_id = oid(adminId)
    pipeline = [
        {"$match": {"adminId": admin_id}},
        {"$unwind": "$blockIds"},
        {"$group": {"_id": "$blockIds"}},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "blockId": "$_id"}},
    ]
    return {"results": list(upvotes.aggregate(pipeline))}

def serialize_log(doc: dict) -> dict:
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    return out

def serialize_admin(doc: dict) -> dict:
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    return out
