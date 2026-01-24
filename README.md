# NoSQL-LOGS Project

## Overview

This project implements a MongoDB-based log ingestion, seeding, and analytics system for large-scale system logs.
It is designed to ingest millions of log entries efficiently, enrich them with metadata, enforce consistency
constraints, and expose analytics through a REST API.

The project follows clean code principles, emphasizes safe re-execution of ingestion, and is structured to scale
to multi-million-record datasets.

---

## Architecture

The system consists of four main parts:

1. Ingest
   - Parses raw log files.
   - Normalizes them into structured MongoDB documents.
   - Uses multiprocessing and batched inserts.
   - Prevents duplicate log insertion across restarts.

2. Seed
   - Generates administrators using Faker.
   - Generates upvotes under strict constraints.
   - Guarantees:
     - At least one third of logs have at least one upvote
     - No administrator has more than 1000 upvotes

3. API
   - REST API built with FastAPI.
   - Provides analytics queries required by the assignment.
   - Performs controlled writes for logs, admins, and upvotes.

4. Infrastructure
   - Docker and Docker Compose based.
   - Single MongoDB instance.
   - One runner container executing ingest and then seed sequentially.

---

## Data Model (MongoDB)

### Collections

### logs
Represents ingested log entries.

Fields:
- _id
- logSet
- ingestKey
- ts
- day
- actionType
- sourceIp
- destIp
- blockId
- sizeBytes
- upvoteCount

### admins
Represents administrators who can cast upvotes.

Fields:
- _id
- username (unique)
- email (unique)
- phone
- totalUpvotes

### upvotes
Represents admin votes on logs.

Fields:
- _id
- adminId
- logId
- ts
- day
- usernameUsed
- emailUsed
- sourceIp
- blockIds

---

## Ingestion Design

### Duplicate Prevention Strategy

Each log document includes the fields:
(logSet, ingestKey)

The ingestKey is a SHA-256 hash derived from:
input_path | line_number | raw_line

A unique compound index on (logSet, ingestKey) ensures that the same log line is not inserted more than once.

This guarantees:
- Restarting Docker Compose does not duplicate logs
- Ingestion can be safely re-run without clearing the database
- Partial ingestion failures do not corrupt existing data

### Performance Techniques

- Multiprocessing per log type
- insert_many with ordered=False
- Large batch sizes
- Index creation after ingestion completes
- No per-line existence checks

---

## Seeding Logic

### Admin Generation
- Uses Faker
- Controlled via environment variables
- Skips generation if admins already exist

### Upvote Constraints
- At least one third of all logs receive one or more upvotes
- No admin exceeds 1000 total upvotes
- Deterministic random seed for reproducibility
- Batched writes with admin-cap enforcement

---

## API

### Health
GET /health

### Writes
- POST /insert-logs
- POST /create-admin
- POST /cast-upvote

### Analytics Endpoints
The API implements all required analytics queries, including:
- Logs per type
- Requests per day
- Top actions per source IP
- Least used HTTP methods
- Referrers with multiple resources
- Replicated and served blocks
- Top upvoted logs
- Top admins by upvotes
- Admins by distinct source IPs
- Logs associated with multiple usernames per email
- Block IDs voted by a given admin

---

## Indexing Strategy

Indexes are explicitly created for:
- Time-based analytics
- Grouping by day
- Sorting by upvotes
- Preventing duplicate ingestion
- Administrator uniqueness
- Efficient aggregation pipelines

Indexes are created once ingestion finishes, to maximize ingestion throughput.

---

## Project Structure

db/
- ingest/
- seed/
- api/
- util/
- Dockerfile
- docker-compose.yml
- README.md

---

## Running the Project

Start everything:
docker compose up --build

This will:
1. Start MongoDB
2. Run ingestion
3. Run seeding
4. Start the API

Reset all data:
docker compose down -v

This removes MongoDB volumes and forces a clean run.

---

## Design Principles Applied

- Clean Code
- Single responsibility per module
- Deterministic execution for grading
- Database-enforced duplicate prevention
- Explicit indexing strategy
- No hidden side effects

---

## Assignment Compliance

- Large-scale ingestion of millions of logs
- Administrator data generated with Faker
- At least one third of logs have at least one upvote
- No administrator has more than 1000 upvotes
- Full analytics coverage
- REST API
- Dockerized and reproducible setup

---

## Notes

- Ingestion is append-safe and restart-safe.
- Seeding assumes ingestion has completed successfully.
- Index build failures indicate real data duplication issues.

---

## Author

Developed as part of M149 – Database Management Systems
University of Athens
