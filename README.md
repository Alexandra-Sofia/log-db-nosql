# Readme draft: mongo db log blabla

## schema? or whatever its called in db design choices blabla
{
  "_id": ObjectId,

  "logSet": "ACCESS" | "HDFS_DATAXCEIVER" | "HDFS_NAMESYSTEM",
  "actionType": "GET" | "POST" | "receiving" | "served" | "replicate" | "update",

  "ts": ISODate,
  "day": "YYYY-MM-DD",

  "sourceIp": "x.x.x.x",
  "destIp": "x.x.x.x",

  "blockId": NumberLong,
  "sizeBytes": NumberLong,

  "access": { ... },      // only for ACCESS
  "upvoteCount": NumberInt
}


The data model follows MongoDB’s document-oriented design principles: each log entry is stored as a self-contained document optimized for aggregation, 
while frequently queried attributes are denormalized to minimize joins and pipeline complexity. 
Redundant fields such as the day string and upvote counters are intentionally introduced to improve query performance and simplify analytics.

### Developer notes

fixed an issue with the line endings in the namesystem log parsing that reduced the number of lines accepted significantly.