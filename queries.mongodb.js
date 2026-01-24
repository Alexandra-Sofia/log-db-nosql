/* ============================================================
   MongoDB Analytics Queries – NoSQL-LOGS Project
   Database: nosql_logs
   Matches current FastAPI analytics logic (Q1 to Q11)
   ============================================================ */

use nosql_logs;

/* ------------------------------------------------------------
   Common parameters (adjust if needed)
------------------------------------------------------------ */
const start = ISODate("2008-11-09T00:00:00Z");
const end   = ISODate("2008-11-11T23:59:59Z");
const day   = "2008-11-09";

const apacheStart = ISODate("2005-06-09T00:00:00Z");
const apacheEnd   = ISODate("2006-02-28T23:59:59Z");

const logSetForQ2 = "HDFS_DATAXCEIVER";
const usernameForQ11 = "some_username";

/* ============================================================
   Q1. Total logs per actionType in a time range (descending)
   Matches: /analytics/logs-per-type
============================================================ */
print("\nQ1: Total logs per actionType");
db.logs.aggregate([
  { $match: { ts: { $gte: start, $lte: end } } },
  { $group: { _id: "$actionType", total: { $sum: 1 } } },
  { $sort: { total: -1 } }
]).toArray();

/* ============================================================
   Q2. Total requests per day for a logSet and time range
   Matches: /analytics/requests-per-day
============================================================ */
print(`\nQ2: Requests per day (${logSetForQ2})`);
db.logs.aggregate([
  { $match: { logSet: logSetForQ2, ts: { $gte: start, $lte: end } } },
  { $group: { _id: "$day", total: { $sum: 1 } } },
  { $sort: { _id: 1 } }
]).toArray();

/* ============================================================
   Q3. Three most common logs per source IP for a day
   Matches: /analytics/top3-per-sourceip
============================================================ */
print("\nQ3: Top 3 logs per source IP");
db.logs.aggregate([
  { $match: { day: day, sourceIp: { $ne: null } } },
  { $addFields: {
      sig: {
        $cond: [
          { $eq: ["$logSet", "ACCESS"] },
          {
            $concat: [
              { $ifNull: ["$access.method", ""] },
              " ",
              { $ifNull: ["$access.resource", ""] }
            ]
          },
          { $ifNull: ["$actionType", "UNKNOWN"] }
        ]
      }
    }
  },
  { $group: { _id: { sourceIp: "$sourceIp", sig: "$sig" }, cnt: { $sum: 1 } } },
  { $sort: { "_id.sourceIp": 1, cnt: -1, "_id.sig": 1 } },
  { $group: { _id: "$_id.sourceIp", top: { $push: { sig: "$_id.sig", cnt: "$cnt" } } } },
  { $project: { _id: 0, sourceIp: "$_id", top: { $slice: ["$top", 3] } } }
]).toArray();

/* ============================================================
   Q4. Two least common HTTP methods in a time range
   Matches: /analytics/least-http-methods
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
   Matches: /analytics/referrers-multi-resource
============================================================ */
print("\nQ5: Referrers with multiple resources");
db.logs.aggregate([
  { $match: { logSet: "ACCESS", "access.referrer": { $nin: [null, "-", ""] } } },
  { $group: { _id: "$access.referrer", resources: { $addToSet: "$access.resource" } } },
  { $project: { resources: 1, resourceCount: { $size: "$resources" } } },
  { $match: { resourceCount: { $gt: 1 } } },
  { $sort: { resourceCount: -1 } }
]).toArray();

/* ============================================================
   Q6. Blocks replicated and served on the same day
   Matches intended behavior of: /analytics/blocks-replicated-and-served
   Note: your current API code has day filter commented out, but the
   project query requires "same day", so this keeps the day filter.
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
      _id: { day: "$day", blockId: "$blockId" },
      hasReplicate: { $max: { $cond: [{ $eq: ["$actionType", "replicate"] }, 1, 0] } },
      hasServed: { $max: { $cond: [{ $eq: ["$actionType", "served"] }, 1, 0] } }
    }
  },
  { $match: { hasReplicate: 1, hasServed: 1 } },
  { $project: { _id: 0, day: "$_id.day", blockId: "$_id.blockId" } },
  { $sort: { day: 1, blockId: 1 } }
]).toArray();

/* ============================================================
   Q7. Fifty most upvoted logs for a specific day
   Matches: /analytics/top-upvoted-logs
============================================================ */
print("\nQ7: Top 50 upvoted logs");
db.logs.find({ day: day }).sort({ upvoteCount: -1 }).limit(50).toArray();

/* ============================================================
   Q8. Fifty most active administrators by total upvotes
   Matches: /analytics/top-admins-upvotes
============================================================ */
print("\nQ8: Top admins by total upvotes");
db.admins.find(
  {},
  { username: 1, email: 1, phone: 1, totalUpvotes: 1 }
).sort({ totalUpvotes: -1 }).limit(50).toArray();

/* ============================================================
   Q9. Top 50 admins by number of distinct source IPs voted
   Matches: /analytics/top-admins-sourceips
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
  { $project: { adminId: { $toString: "$_id" }, username: "$admin.username", email: "$admin.email", ipCount: 1 } }
]).toArray();

/* ============================================================
   Q10. Logs where the same email used with more than one username
   Matches: /analytics/logs-multi-username-per-email
============================================================ */
print("\nQ10: Logs where same email used with multiple usernames");
db.upvotes.aggregate([
  { $group: {
      _id: { email: "$emailUsed", logId: "$logId" },
      usernames: { $addToSet: "$usernameUsed" }
    }
  },
  { $match: { $expr: { $gt: [{ $size: "$usernames" }, 1] } } },
  { $lookup: {
      from: "logs",
      localField: "_id.logId",
      foreignField: "_id",
      as: "logDetails"
    }
  },
  { $unwind: "$logDetails" },
  { $project: {
      _id: 0,
      flaggedEmail: "$_id.email",
      logId: { $toString: "$_id.logId" },
      usernamesUsed: "$usernames",
      logContent: "$logDetails"
    }
  }
]).toArray();

/* ============================================================
   Q11. Block IDs voted by a given username
   Matches: /analytics/blockids-voted
============================================================ */
print("\nQ11: Block IDs voted by a username");
db.upvotes.aggregate([
  { $match: { usernameUsed: usernameForQ11 } },
  { $unwind: "$blockIds" },
  { $group: { _id: "$blockIds" } },
  { $sort: { _id: 1 } },
  { $project: { _id: 0, blockId: "$_id" } }
]).toArray();
