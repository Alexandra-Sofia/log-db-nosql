import os
from multiprocessing import Process
from typing import Optional

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

"""
Ingestion entrypoint.

This module coordinates ingestion of multiple log sources into a single MongoDB
collection by spawning one process per log type:
    * Apache access logs
    * HDFS DataXceiver logs
    * HDFS FSNamesystem logs

After all workers complete successfully, it creates the required indexes.
"""


def _env(name: str, default: str) -> str:
    """
    Read an environment variable, falling back to a default if unset or blank.

    :param name: Environment variable name.
    :param default: Default value returned when missing or blank.
    :return: str
    """
    value = os.getenv(name)
    return value if value and value.strip() else default


def _build_paths(logdir: str) -> tuple[str, str, str]:
    """
    Build absolute input paths for all required log files.

    :param logdir: Base directory that contains input log files.
    :return: tuple[str, str, str]
    """
    access_log = os.path.join(logdir, ACCESS_FILENAME)
    datax_log = os.path.join(logdir, DATAX_FILENAME)
    namesys_log = os.path.join(logdir, NAMESYS_FILENAME)
    return access_log, datax_log, namesys_log


def _start_workers(
    access_log: str,
    datax_log: str,
    namesys_log: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    batch_size: int,
) -> tuple[Process, Process, Process]:
    """
    Start ingestion worker processes for each log type.

    :param access_log: Path to the Apache access log file.
    :param datax_log: Path to the HDFS DataXceiver log file.
    :param namesys_log: Path to the HDFS FSNamesystem log file.
    :param mongo_uri: MongoDB connection URI.
    :param mongo_db: Target MongoDB database name.
    :param mongo_coll: Target MongoDB collection name.
    :param batch_size: Batch size for bulk inserts.
    :return: tuple[Process, Process, Process]
    """
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

    return p1, p2, p3


def _join_workers(processes: tuple[Process, ...]) -> None:
    """
    Wait for all worker processes to complete and fail fast if any failed.

    :param processes: Worker processes to join.
    :return: None
    """
    for p in processes:
        p.join()

    failed = [p.exitcode for p in processes if p.exitcode != 0]
    if failed:
        tiny_logger(f"[INGEST] Worker failure exit codes: {failed}")
        raise SystemExit(1)


def _create_indexes(mongo_uri: str, mongo_db: str, mongo_coll: str) -> None:
    """
    Create the required MongoDB indexes after ingestion completes.

    :param mongo_uri: MongoDB connection URI.
    :param mongo_db: Target MongoDB database name.
    :param mongo_coll: Target MongoDB collection name.
    :return: None
    """
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    ensure_indexes(db, logs_coll=mongo_coll)


def main(
    logdir: Optional[str] = None,
    mongo_uri: Optional[str] = None,
    mongo_db: Optional[str] = None,
    mongo_coll: Optional[str] = None,
    batch_size: int = BATCH_SIZE_DEFAULT,
) -> None:
    """
    Ingest all log sources into MongoDB.

    The function resolves configuration (either explicit arguments or
    environment variables), spawns worker processes to parse each log file,
    validates successful completion, and then creates required indexes.

    Environment variables:
        * LOG_DIR
        * MONGO_URI
        * MONGO_DB
        * MONGO_COLLECTION

    :param logdir: Directory containing log files. Defaults from LOG_DIR.
    :param mongo_uri: MongoDB connection URI. Defaults from MONGO_URI.
    :param mongo_db: MongoDB database name. Defaults from MONGO_DB.
    :param mongo_coll: MongoDB collection name. Defaults from MONGO_COLLECTION.
    :param batch_size: Batch size used by workers for bulk inserts.
    :return: None
    """
    resolved_logdir = logdir or _env("LOG_DIR", "/input_logs")
    resolved_uri = mongo_uri or _env("MONGO_URI", "mongodb://mongo:27017")
    resolved_db = mongo_db or _env("MONGO_DB", "nosql_logs")
    resolved_coll = mongo_coll or _env("MONGO_COLLECTION", "logs")

    access_log, datax_log, namesys_log = _build_paths(resolved_logdir)

    tiny_logger(f"[INGEST] logdir={resolved_logdir}")
    tiny_logger(f"[INGEST] collection={resolved_db}.{resolved_coll}")
    tiny_logger("[INGEST] Starting workers...")

    workers = _start_workers(
        access_log=access_log,
        datax_log=datax_log,
        namesys_log=namesys_log,
        mongo_uri=resolved_uri,
        mongo_db=resolved_db,
        mongo_coll=resolved_coll,
        batch_size=batch_size,
    )

    _join_workers(workers)

    tiny_logger("[INGEST] Workers succeeded. Creating indexes...")
    _create_indexes(resolved_uri, resolved_db, resolved_coll)
    tiny_logger("[INGEST] Index creation complete. Ingest finished.")


if __name__ == "__main__":
    main()
