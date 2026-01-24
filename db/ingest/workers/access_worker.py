import re
from typing import Any, Dict, List

from pymongo import MongoClient

from timestamps import ts_apache, day_str
from writer import flush_batch
from util import tiny_logger, LogType

"""
Apache access log ingestion worker.

This module parses Apache access log files line by line, converts valid
entries into structured MongoDB documents, and inserts them in batches.
Invalid or unparsable lines are skipped silently.
"""

ACCESS_REGEX = re.compile(
    r'(?P<ip>\S+)\s+'
    r'(?P<remote_name>\S+)\s+'
    r'(?P<auth_user>\S+)\s+'
    r'\[(?P<timestamp>[^]]+)\]\s+'
    r'"(?P<method>[A-Za-z]+)\s+(?P<resource>[^"]+?)\s+HTTP/[^"]+"\s+'
    r'(?P<status>\d{3})\s+'
    r'(?P<size>\S+)\s+'
    r'"(?P<referrer>[^"]*)"\s+'
    r'"(?P<agent>[^"]*)"'
)


def parse_access_worker(
    input_path: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    batch_size: int,
) -> None:
    """
    Parse an Apache access log file and insert entries into MongoDB.

    The function reads the log file line by line, applies a regular
    expression to extract fields, transforms them into a normalized
    document schema, and inserts documents into MongoDB in batches.

    Lines that do not match the expected access log format are ignored.

    :param input_path: Path to the Apache access log file.
    :param mongo_uri: MongoDB connection URI.
    :param mongo_db: Target MongoDB database name.
    :param mongo_coll: Target MongoDB collection name.
    :param batch_size: Number of documents per bulk insert.
    :return: None
    """
    client = MongoClient(mongo_uri)
    collection = client[mongo_db][mongo_coll]

    tiny_logger(f"[ACCESS] start: {input_path}")

    batch: List[Dict[str, Any]] = []
    total_lines = 0
    matched_lines = 0
    inserted_docs = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        for raw_line in infile:
            total_lines += 1
            line = raw_line.rstrip("\n")

            match = ACCESS_REGEX.match(line)
            if match is None:
                continue

            matched_lines += 1
            groups = match.groupdict()

            timestamp = ts_apache(groups["timestamp"])
            size_bytes = None if groups["size"] in {"", "-"} else int(groups["size"])
            referrer = None if groups["referrer"] == "-" else groups["referrer"]

            document = {
                "logSet": LogType.ACCESS,
                "actionType": groups["method"],
                "ts": timestamp,
                "day": day_str(timestamp),
                "sourceIp": groups["ip"],
                "destIp": None,
                "blockId": None,
                "sizeBytes": size_bytes,
                "access": {
                    "remoteName": groups["remote_name"],
                    "authUser": groups["auth_user"],
                    "method": groups["method"],
                    "resource": groups["resource"],
                    "status": int(groups["status"]),
                    "referrer": referrer if referrer is not None else "-",
                    "userAgent": groups["agent"],
                },
                "upvoteCount": 0,
            }

            batch.append(document)

            if len(batch) >= batch_size:
                inserted_docs += flush_batch(collection, batch)
                batch.clear()

    inserted_docs += flush_batch(collection, batch)

    tiny_logger(
        f"[ACCESS] done: matched {matched_lines}/{total_lines}, "
        f"inserted {inserted_docs}"
    )
