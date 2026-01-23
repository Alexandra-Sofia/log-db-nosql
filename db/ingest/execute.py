import os
from multiprocessing import Process

from pymongo import MongoClient

from config import (
    ACCESS_FILENAME,
    DATAX_FILENAME,
    NAMESYS_FILENAME,
    BATCH_SIZE_DEFAULT,
)
from indexes import ensure_indexes
from util import tiny_logger
from workers.access_worker import parse_access_worker
from workers.dataxceiver_worker import parse_dataxceiver_worker
from workers.namesystem_worker import parse_namesystem_worker


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v and v.strip() else default


def main(
    logdir: str | None = None,
    mongo_uri: str | None = None,
    mongo_db: str | None = None,
    mongo_coll: str | None = None,
    batch_size: int = BATCH_SIZE_DEFAULT,
) -> None:
    logdir = logdir or _env("LOG_DIR", "/input_logs")
    mongo_uri = mongo_uri or _env("MONGO_URI", "mongodb://mongo:27017")
    mongo_db = mongo_db or _env("MONGO_DB", "nosql_logs")
    mongo_coll = mongo_coll or _env("MONGO_COLLECTION", "logs")

    access_log = os.path.join(logdir, ACCESS_FILENAME)
    datax_log = os.path.join(logdir, DATAX_FILENAME)
    namesys_log = os.path.join(logdir, NAMESYS_FILENAME)

    tiny_logger(f"[INGEST] logdir={logdir}")
    tiny_logger(f"[INGEST] collection={mongo_db}.{mongo_coll}")
    tiny_logger("[INGEST] Starting workers...")

    p1 = Process(
        target=parse_access_worker,
        args=(access_log, mongo_uri, mongo_db, mongo_coll, batch_size),
    )
    p2 = Process(
        target=parse_dataxceiver_worker,
        args=(datax_log, mongo_uri, mongo_db, mongo_coll, batch_size),
    )
    p3 = Process(
        target=parse_namesystem_worker,
        args=(namesys_log, mongo_uri, mongo_db, mongo_coll, batch_size),
    )

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()

    # Fail if any worker failed
    failed = [p.exitcode for p in (p1, p2, p3) if p.exitcode != 0]
    if failed:
        tiny_logger(f"[INGEST] Worker failure exit codes: {failed}")
        raise SystemExit(1)

    tiny_logger("[INGEST] Workers succeeded. Creating indexes...")

    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    ensure_indexes(db, logs_coll=mongo_coll)

    tiny_logger("[INGEST] Index creation complete. Ingest finished.")


if __name__ == "__main__":
    main()
