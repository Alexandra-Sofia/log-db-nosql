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
- Docker Compose v
- Unix based OS

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
├── Dockerfile
├── README.md
├── docker-compose.yml
├── ingest/                 # Log ingestion logic
│   ├── execute.py
│   ├── indexes.py
│   └── workers/            # Parsers per log type
│       ├── access_worker.py
│       ├── dataxceiver_worker.py
│       └── namesystem_worker.py
├── seed/                   # Admin and upvote generation
│   └── execute.py
├── api/                    # FastAPI service
│   ├── main.py
│   ├── schemas.py
│   └── db.py
├── util/                   # Shared utilities
├── input-logfiles/         # Raw log files
└── requirements.txt
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
