# NoSQL-LOGS Ingestion & Querying Pipeline

This README covers:
- Directory structure
- Schema overview (included below)
- Parsing logic
- File formats
- Ingestion workflow
- Docker Compose usage
- REST API overview
- Development notes

#### 🔧 System Requirements
- Docker Engine or Docker Desktop
- Docker Compose v2
- Unix based Docker Host

---

## 1. Overview

NoSQL-LOGS is a high‑performance log ingestion, seeding, and analytics pipeline designed for large heterogeneous server logs.
It provides an end‑to‑end workflow that parses raw log files, normalizes them into structured MongoDB documents,
and exposes analytics through a REST API.

The system processes three log categories:

1. **Apache/HTTP ACCESS logs**
2. **HDFS DataXceiver logs**
3. **HDFS NameSystem logs**

The pipeline:
- Parses logs using parallel worker processes.
- Normalizes log formats into a unified schema.
- Inserts data in large batches for performance.
- Seeds administrators and upvotes under strict constraints.
- Exposes required analytics queries via FastAPI.
- Is fully containerized and deployed via Docker Compose.

---

## 2. Directory Structure

```
.
├── README.md                     # Project documentation
├── docker-compose.yml            # Orchestrates MongoDB, ingest/seed runner, and API
├── example-api-calls.sh          # Example curl commands for API analytics endpoints
├── queries.mongodb.js            # MongoDB shell queries (Q1–Q11)
├── queries_and_results.mongodb.js# MongoDB queries with sample outputs
├── input-logfiles                # Raw input log files for ingestion
│   ├── access_log_full           # Apache/HTTP access logs
│   ├── HDFS_DataXceiver.log      # HDFS DataXceiver logs
│   ├── HDFS_FS_Namesystem.log    # HDFS NameSystem logs
│   └── logs.tar.gz               # Archived copy of the original datasets
├── api                           # FastAPI service exposing analytics and write endpoints
│   ├── Dockerfile                # API container definition
│   ├── requirements.txt          # API Python dependencies
│   ├── db.py                     # MongoDB connection setup
│   ├── schemas.py                # Pydantic models for requests and validation
│   └── main.py                   # API entry point and analytics endpoints (Q1–Q11)
├── db                            # Database-related logic (ingest and seed)
│   ├── Dockerfile                # Runner container for ingest + seed
│   ├── requirements.txt          # Shared Python dependencies
│   ├── __init__.py
│   ├── ingest                    # Log ingestion pipeline
│   │   ├── config.py             # Ingestion configuration and defaults
│   │   ├── execute.py            # Ingest entry point (spawns workers, builds indexes)
│   │   ├── indexes.py            # MongoDB index definitions
│   │   ├── timestamps.py         # Timestamp parsing and normalization helpers
│   │   ├── util.py               # Shared utilities and enums
│   │   ├── writer.py             # Batched MongoDB insert logic
│   │   └── workers               # Dedicated parsers per log type
│   │       ├── access_worker.py  # Apache ACCESS log parser
│   │       ├── dataxceiver_worker.py # HDFS DataXceiver log parser
│   │       └── namesystem_worker.py  # HDFS NameSystem log parser
│   └── seed                      # Database seeding logic
│       ├── common.py             # Shared seed utilities and configuration
│       ├── execute.py            # Seed entry point (admins + upvotes)
│       ├── admins.py             # Administrator generation (Faker-based)
│       ├── upvotes.py            # Upvote generation with coverage and cap constraints
│       └── tiny_logger.py        # Minimal timestamped logging utility

```

---

## 3. MongoDB Schema Overview

### logs collection
Stores all ingested log entries in a unified format.

Fields:
- `_id` (ObjectId)
- `logSet` (ACCESS | HDFS_DATAXCEIVER | HDFS_NAMESYSTEM)
- `ingestKey` (string, unique per logSet)
- `ts` (datetime, UTC)
- `day` (YYYY-MM-DD string)
- `actionType` (HTTP method or HDFS action)
- `sourceIp` (string, optional)
- `destIp` (string, optional)
- `blockId` (int, optional)
- `sizeBytes` (int, optional)
- `access` (subdocument, ACCESS only)
- `upvoteCount` (int)

Justification:
A single collection simplifies cross-log analytics while optional fields
allow heterogeneous log types without joins.

### admins collection
Represents administrators who cast upvotes.

Fields:
- `_id`
- `username` (unique)
- `email` (unique)
- `phone`
- `totalUpvotes`

### upvotes collection
Represents administrator votes on logs.

Fields:
- `_id`
- `adminId`
- `logId`
- `ts`
- `day`
- `usernameUsed`
- `emailUsed`
- `sourceIp`
- `blockIds` (array)

Denormalized fields are stored to support analytics without joins.

---

## 4. Ingestion Workflow

### A. Parse and ingest logs
Executed inside the ingest container:

```
python execute.py
```

Three worker processes run in parallel:

| Worker | Input File | Notes |
|------|-----------|-------|
| ACCESS | access_log_full | Parses HTTP metadata |
| DATAX | HDFS_DataXceiver.log | receiving / received / served |
| NAMESYS | HDFS_FS_Namesystem.log | update / replicate |

Each worker:
- Parses lines using regex
- Normalizes fields
- Inserts documents in large batches

Indexes are created **after ingestion** to maximize throughput.

### B. Duplicate protection
Each log includes an `ingestKey` derived from file path, line number, and content.
A unique index on `(logSet, ingestKey)` prevents duplicate insertion across restarts.

---

## 5. Seeding Workflow

The seed step runs after ingestion:

- Generates administrators using Faker.
- Generates upvotes with constraints:
  - ≥ 1/3 of logs receive at least one upvote
  - No admin exceeds 1000 upvotes
- Uses deterministic randomness for reproducibility.

### Example Execution flow for the ingest and seed scripts:

Output from `docker logs -f  log-db-nosql-runner-1`

```commandline
2026-01-24 11:41:08.250 | Pipeline: START.
2026-01-24 11:41:08.252 | Ingest step deployment: START.
2026-01-24 11:41:08.925 | [INGEST] logdir=/input-logfiles
2026-01-24 11:41:08.925 | [INGEST] collection=nosql_logs.logs
2026-01-24 11:41:08.925 | [INGEST] Starting workers...
2026-01-24 11:41:08.955 | [DATAX] start: /input-logfiles/HDFS_DataXceiver.log
2026-01-24 11:41:08.955 | [NAMESYS] start: /input-logfiles/HDFS_FS_Namesystem.log
2026-01-24 11:41:08.956 | [ACCESS] start: /input-logfiles/access_log_full
2026-01-24 11:41:10.603 | [ACCESS] done: matched 36044/36310, inserted 36044
2026-01-24 11:42:15.881 | [NAMESYS] done: matched 1726743/3700245, inserted 1726908
2026-01-24 11:42:19.216 | [DATAX] done: matched 2159055/2518678, inserted 2159055
2026-01-24 11:42:19.220 | [INGEST] Workers succeeded. Creating indexes...
2026-01-24 11:43:10.309 | [INGEST] Index creation complete. Ingest finished.
2026-01-24 11:43:10.814 | Ingest step deployment: END.
2026-01-24 11:43:10.816 | Seed step deployment: START.
2026-01-24 11:43:11.625 | [SEED] START
2026-01-24 11:43:11.625 | [SEED] MODE=fail SEED=42 DB=nosql_logs
2026-01-24 11:43:11.629 | [SEED][INDEXES] Ensuring core indexes...
2026-01-24 11:43:16.791 | [SEED][INDEXES] Core indexes ensured.
2026-01-24 11:43:18.107 | [SEED][ADMINS] Generating 1500 administrators...
2026-01-24 11:43:18.619 | [SEED][ADMINS] Inserted 1500 admins.
2026-01-24 11:43:19.917 | [SEED] After admins: logs=3922007 admins=1500 upvotes=0 inserted_admins=1500
2026-01-24 11:43:19.917 | [SEED][UPVOTES] START
2026-01-24 11:43:21.189 | [SEED][UPVOTES] Logs: 3922007
2026-01-24 11:43:21.189 | [SEED][UPVOTES] Admins: 1500
2026-01-24 11:43:21.189 | [SEED][UPVOTES] Existing upvotes: 0
2026-01-24 11:43:21.189 | [SEED][UPVOTES] Covered logs now: 0 (need at least 1307336)
2026-01-24 11:43:21.189 | [SEED][UPVOTES] Need additional covered logs: 1307336
2026-01-24 11:43:21.189 | [SEED][UPVOTES] Target total votes: 2091738 (EXTRA_VOTE_FRAC=0.2)
2026-01-24 11:43:21.198 | [SEED][UPVOTES] Ensuring coverage: inserting at least 1307336 new votes on 0-vote logs...
2026-01-24 11:43:27.596 | [SEED][UPVOTES] Coverage progress: 200000/1307336
2026-01-24 11:43:38.822 | [SEED][UPVOTES] Coverage progress: 400000/1307336
2026-01-24 11:43:49.277 | [SEED][UPVOTES] Coverage progress: 600000/1307336
2026-01-24 11:43:58.899 | [SEED][UPVOTES] Coverage progress: 800000/1307336
2026-01-24 11:44:11.328 | [SEED][UPVOTES] Coverage progress: 1000000/1307336
2026-01-24 11:44:24.046 | [SEED][UPVOTES] Coverage progress: 1200000/1307336
2026-01-24 11:44:31.694 | [SEED][UPVOTES] Coverage step complete: inserted 1307336 votes.
2026-01-24 11:44:31.698 | [SEED][UPVOTES] Adding extra votes for richness: remaining=192664
2026-01-24 11:44:31.698 | [SEED][UPVOTES] Building sampling pool of logs (pool_size=200000)...
2026-01-24 11:44:43.516 | [SEED][UPVOTES] Extra vote step complete.
2026-01-24 11:44:43.563 | [SEED][UPVOTES] Recomputing counters from upvotes collection...
2026-01-24 11:45:48.092 | [SEED][UPVOTES] Updated logs.upvoteCount for 1307336 logs.
2026-01-24 11:45:49.298 | [SEED][UPVOTES] Updated admins.totalUpvotes for 1500 admins.
2026-01-24 11:45:50.053 | [SEED][UPVOTES] Inserted votes this run: 1498507
2026-01-24 11:45:50.053 | [SEED][UPVOTES] Logs with >=1 upvote: 1307336 (required >= 1307336)
2026-01-24 11:45:50.053 | [SEED][UPVOTES] Max admin totalUpvotes: 1000 (cap 1000)
2026-01-24 11:45:50.053 | [SEED][UPVOTES] END
2026-01-24 11:45:52.671 | [SEED] After upvotes: logs=3922007 admins=1500 upvotes=1498507 inserted_votes=1498507
2026-01-24 11:45:52.671 | [SEED] END
2026-01-24 11:45:53.146 | Seed step deployment: END.
2026-01-24 11:45:53.149 | Pipeline: END.
```

---

## 6. REST API

The FastAPI service exposes:

### Health
- `GET /health`

### Writes
- `POST /insert-logs`
- `POST /create-admin`
- `POST /cast-upvote`

### Analytics
Endpoints implement all required queries, including:
- Logs per type
- Requests per day
- Top actions per source IP
- Least common HTTP methods
- Referrers with multiple resources
- Blocks replicated and served the same day
- Top upvoted logs
- Top administrators by upvotes
- Admins by distinct source IPs
- Logs with multiple usernames per email
- Block IDs voted by a given username

### API Access

Once Docker Compose is running, the REST API is available at:

http://localhost:8000

Interactive API documentation (Swagger UI):

http://localhost:8000/docs


---

## 7. Docker Compose Usage

Start the full pipeline:

```
docker compose up --build
```

This:
1. Starts MongoDB
2. Runs ingestion
3. Runs seeding
4. Starts the API

Reset everything:

```
docker compose down -v
```

---

## 8. Development Notes

- All three log files need to be manually copied to the input-logfiles directory.
- Ingestion is restart-safe due to database-level uniqueness.
- Index build failures indicate real duplicate data.
- The schema is designed to favor read-heavy analytics workloads.
- Adding a new log type requires:
  - New parser
  - Field mapping
  - Optional analytics extensions

---

## 9. License

Internal academic project. No license.

---

## 10. Author

NoSQL-LOGS ingestion, MongoDB schema design, seeding logic, and FastAPI analytics
developed by Sofia, Alexandra 
2025

---
