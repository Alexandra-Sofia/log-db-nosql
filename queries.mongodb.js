/* ============================================================
   MongoDB Analytics Queries – NoSQL-LOGS Project
   Database: nosql_logs
   ============================================================ */

use nosql_logs;

/* ------------------------------------------------------------
   Common parameters (adjust if needed)
------------------------------------------------------------ */
const hdfsStart = ISODate("2008-11-09T00:00:00Z");
const hdfsEnd   = ISODate("2008-11-11T23:59:59Z");
const apacheStart = ISODate("2005-06-09T00:00:00Z");
const apacheEnd   = ISODate("2006-02-28T23:59:59Z");
const day = "2008-11-09";

/* ============================================================
   Q1. Total logs per type in a time range (descending)
============================================================ */
print("\nQ1: Total logs per type");
db.logs.aggregate([
  { $match: { ts: { $gte: hdfsStart, $lte: hdfsEnd } } },
  { $group: { _id: "$actionType", total: { $sum: 1 } } },
  { $sort: { total: -1 } }
]).toArray();

/* ============================================================
   Q2. Total requests per day for a log type and time range
============================================================ */
print("\nQ2: Requests per day (HDFS_DATAXCEIVER)");
db.logs.aggregate([
  { $match: { logSet: "HDFS_DATAXCEIVER", ts: { $gte: hdfsStart, $lte: hdfsEnd } } },
  { $group: { _id: "$day", total: { $sum: 1 } } },
  { $sort: { _id: 1 } }
]).toArray();

/* ============================================================
   Q3. Three most common logs per source IP for a day
============================================================ */
print("\nQ3: Top 3 logs per source IP");
db.logs.aggregate([
  { $match: { day: day, sourceIp: { $ne: null } } },
  { $addFields: {
      sig: {
        $cond: [
          { $eq: ["$logSet", "ACCESS"] },
          { $concat: ["$access.method", " ", "$access.resource"] },
          "$actionType"
        ]
      }
    }
  },
  { $group: { _id: { sourceIp: "$sourceIp", sig: "$sig" }, cnt: { $sum: 1 } } },
  { $sort: { "_id.sourceIp": 1, cnt: -1 } },
  { $group: { _id: "$_id.sourceIp", top: { $push: { sig: "$_id.sig", cnt: "$cnt" } } } },
  { $project: { _id: 0, sourceIp: "$_id", top: { $slice: ["$top", 3] } } }
]).toArray();

/* ============================================================
   Q4. Two least common HTTP methods in a time range
============================================================ */
print("\nQ4: Two least common HTTP methods");
db.logs.aggregate([
  { $match: { logSet: "ACCESS", ts: { $gte: apacheStart, $lte: apacheEnd } } },
  { $group: { _id: "$access.method", cnt: { $sum: 1 } } },
  { $sort: { cnt: 1 } },
  { $limit: 2 }
]).toArray();

/* ============================================================
   Q5. Referrers leading to more than one resource
============================================================ */
print("\nQ5: Referrers with multiple resources");
db.logs.aggregate([
  { $match: { logSet: "ACCESS", "access.referrer": { $nin: [null, "-", ""] } } },
  { $group: { _id: "$access.referrer", resources: { $addToSet: "$access.resource" } } },
  { $project: { resourceCount: { $size: "$resources" }, resources: 1 } },
  { $match: { resourceCount: { $gt: 1 } } },
  { $sort: { resourceCount: -1 } }
]).toArray();

/* ============================================================
   Q6. Blocks replicated and served on the same day
============================================================ */
print("\nQ6: Blocks replicated and served same day");
db.logs.aggregate([
  { $match: {
      day: day,
      logSet: { $in: ["HDFS_NAMESYSTEM", "HDFS_DATAXCEIVER"] },
      actionType: { $in: ["replicate", "served"] },
      blockId: { $ne: null }
    }
  },
  { $group: {
      _id: "$blockId",
      replicated: { $max: { $cond: [{ $eq: ["$actionType", "replicate"] }, 1, 0] } },
      served:     { $max: { $cond: [{ $eq: ["$actionType", "served"] }, 1, 0] } }
    }
  },
  { $match: { replicated: 1, served: 1 } },
  { $project: { _id: 0, blockId: "$_id" } }
]).toArray();

/* ============================================================
   Q7. Fifty most upvoted logs for a day
============================================================ */
print("\nQ7: Top 50 upvoted logs");
db.logs.find({ day: day }).sort({ upvoteCount: -1 }).limit(50).toArray();

/* ============================================================
   Q8. Fifty most active administrators by upvotes
============================================================ */
print("\nQ8: Top admins by total upvotes");
db.admins.find(
  {},
  { username: 1, email: 1, phone: 1, totalUpvotes: 1 }
).sort({ totalUpvotes: -1 }).limit(50).toArray();

/* ============================================================
   Q9. Top admins by number of distinct source IPs voted
============================================================ */
print("\nQ9: Top admins by distinct source IPs");
db.upvotes.aggregate([
  { $match: { sourceIp: { $ne: null } } },
  { $group: { _id: "$adminId", ips: { $addToSet: "$sourceIp" } } },
  { $project: { ipCount: { $size: "$ips" } } },
  { $sort: { ipCount: -1 } },
  { $limit: 50 },
  { $lookup: { from: "admins", localField: "_id", foreignField: "_id", as: "admin" } },
  { $unwind: "$admin" },
  { $project: { username: "$admin.username", email: "$admin.email", ipCount: 1 } }
]).toArray();

/* ============================================================
   Q10. Logs where same email used with multiple usernames
============================================================ */
print("\nQ10: Logs with multiple usernames per email");
db.upvotes.aggregate([
  { $group: {
      _id: "$emailUsed",
      usernames: { $addToSet: "$usernameUsed" },
      logIds: { $addToSet: "$logId" }
    }
  },
  { $project: { usernameCount: { $size: "$usernames" }, logIds: 1 } },
  { $match: { usernameCount: { $gt: 1 } } },
  { $unwind: "$logIds" },
  { $lookup: { from: "logs", localField: "logIds", foreignField: "_id", as: "log" } },
  { $unwind: "$log" }
]).toArray();

/* ============================================================
   Q11. Block IDs voted by a given username
============================================================ */
print("\nQ11: Block IDs voted by a username");
const username = db.admins.findOne()?.username;

db.upvotes.aggregate([
  { $match: { usernameUsed: username } },
  { $unwind: "$blockIds" },
  { $group: { _id: "$blockIds" } },
  { $sort: { _id: 1 } },
  { $project: { _id: 0, blockId: "$_id" } }
]).toArray();
