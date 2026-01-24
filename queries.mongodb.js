/* ============================================================
   MongoDB Analytics Queries – NoSQL-LOGS Project
   Database: nosql_logs
   Copy-paste friendly (no JS variables)
   ============================================================ */

use nosql_logs;

/* ============================================================
   Q1. Total logs per actionType in a time range (descending)
   Time range: 2008-11-09 .. 2008-11-11 (UTC)
============================================================ */
print("\nQ1: Total logs per actionType (2008-11-09..2008-11-11)");
db.logs.aggregate([
  { $match: { ts: { $gte: ISODate("2008-11-09T00:00:00Z"), $lte: ISODate("2008-11-11T23:59:59Z") } } },
  { $group: { _id: "$actionType", total: { $sum: 1 } } },
  { $sort: { total: -1 } }
]).toArray();

/* ============================================================
   Q2. Total requests per day for a logSet and time range
   logSet: HDFS_DATAXCEIVER
   Time range: 2008-11-09 .. 2008-11-11 (UTC)
============================================================ */
print("\nQ2: Requests per day (HDFS_DATAXCEIVER, 2008-11-09..2008-11-11)");
db.logs.aggregate([
  { $match: { logSet: "HDFS_DATAXCEIVER", ts: { $gte: ISODate("2008-11-09T00:00:00Z"), $lte: ISODate("2008-11-11T23:59:59Z") } } },
  { $group: { _id: "$day", total: { $sum: 1 } } },
  { $sort: { _id: 1 } }
]).toArray();

/* ============================================================
   Q3. Three most common logs per source IP for a day
   Day: 2008-11-09
============================================================ */
print("\nQ3: Top 3 logs per source IP (day=2008-11-09)");
db.logs.aggregate([
  { $match: { day: "2008-11-09", sourceIp: { $ne: null } } },
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
   Time range: 2005-06-09 .. 2006-02-28 (UTC)
============================================================ */
print("\nQ4: Two least common HTTP methods (2005-06-09..2006-02-28)");
db.logs.aggregate([
  { $match: { logSet: "ACCESS", ts: { $gte: ISODate("2005-06-09T00:00:00Z"), $lte: ISODate("2006-02-28T23:59:59Z") } } },
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
  { $project: { resources: 1, resourceCount: { $size: "$resources" } } },
  { $match: { resourceCount: { $gt: 1 } } },
  { $sort: { resourceCount: -1 } }
]).toArray();

/* ============================================================
   Q6. Blocks replicated and served on the same day
   Day: 2008-11-09
============================================================ */
print("\nQ6: Blocks replicated and served same day (day=2008-11-09)");
db.logs.aggregate([
  { $match: {
      day: "2008-11-09",
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
   Q7. Fifty most upvoted logs for a day
   Day: 2008-11-09
============================================================ */
print("\nQ7: Top 50 upvoted logs (day=2008-11-09)");
db.logs.find({ day: "2008-11-09" }).sort({ upvoteCount: -1 }).limit(50).toArray();

/* ============================================================
   Q8. Fifty most active administrators by total upvotes
============================================================ */
print("\nQ8: Top admins by total upvotes");
db.admins.find(
  {},
  { username: 1, email: 1, phone: 1, totalUpvotes: 1 }
).sort({ totalUpvotes: -1 }).limit(50).toArray();

/* ============================================================
   Q9. Top 50 admins by number of distinct source IPs voted
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
   Username: "some_username"
============================================================ */
print("\nQ11: Block IDs voted by usernameUsed='some_username'");
db.upvotes.aggregate([
  { $match: { usernameUsed: "some_username" } },
  { $unwind: "$blockIds" },
  { $group: { _id: "$blockIds" } },
  { $sort: { _id: 1 } },
  { $project: { _id: 0, blockId: "$_id" } }
]).toArray();
