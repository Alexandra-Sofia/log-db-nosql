import hashlib
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


def _ingest_key(input_path: str, line_no: int, raw_line: str) -> str:
    """
    Build a deterministic per-line ingestion key.

    :param input_path: Input file path.
    :param line_no: 1-based line number.
    :param raw_line: Raw line content (without trailing newline).
    :return: str
    """
    material = f"{input_path}|{line_no}|{raw_line}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def parse_access_worker(
    input_path: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    batch_size: int,
) -> None:
    """
    Parse an Apache access log file and insert documents into MongoDB.

    :param input_path: Path to the input log file.
    :param mongo_uri: MongoDB connection URI.
    :param mongo_db: MongoDB database name.
    :param mongo_coll: MongoDB collection name.
    :param batch_size: Maximum batch size for bulk writes.
    :return: None
    """
    client = MongoClient(mongo_uri)
    coll = client[mongo_db][mongo_coll]

    tiny_logger(f"[ACCESS] start: {input_path}")

    batch: List[Dict[str, Any]] = []
    total = 0
    matched = 0
    inserted = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        for line_no, raw in enumerate(infile, start=1):
            total += 1
            line = raw.rstrip("\n")

            m = ACCESS_REGEX.match(line)
            if not m:
                continue

            matched += 1
            g = m.groupdict()

            ts = ts_apache(g["timestamp"])
            size_bytes = None if g["size"] in {"", "-"} else int(g["size"])
            ref = None if g["referrer"] == "-" else g["referrer"]

            doc = {
                "logSet": LogType.ACCESS,
                "ingestKey": _ingest_key(input_path, line_no, line),
                "actionType": g["method"],
                "ts": ts,
                "day": day_str(ts),
                "sourceIp": g["ip"],
                "destIp": None,
                "blockId": None,
                "sizeBytes": size_bytes,
                "access": {
                    "remoteName": g["remote_name"],
                    "authUser": g["auth_user"],
                    "method": g["method"],
                    "resource": g["resource"],
                    "status": int(g["status"]),
                    "referrer": ref if ref is not None else "-",
                    "userAgent": g["agent"],
                },
                "upvoteCount": 0,
            }

            batch.append(doc)
            if len(batch) >= batch_size:
                inserted += flush_batch(coll, batch)
                batch = []

    inserted += flush_batch(coll, batch)
    tiny_logger(f"[ACCESS] done: matched {matched}/{total}, inserted {inserted}")
