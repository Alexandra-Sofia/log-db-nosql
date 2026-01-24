import hashlib
import re
from typing import Any, Dict, List, Optional

from pymongo import MongoClient

from timestamps import ts_hdfs_compact, day_str
from writer import flush_batch
from util import tiny_logger, LogType

"""
HDFS DataXceiver ingestion worker.

This module parses HDFS DataNode DataXceiver INFO log lines and normalizes three
operation variants into a single MongoDB document shape:
    * Receiving block ...
    * Received block ... (optional size)
    * <src> Served block ... to <dst>

Lines that do not match the expected patterns are ignored.
"""

DATAX_REGEX = re.compile(
    r"""
    ^(?P<date>\d{6})\s+
    (?P<time>\d{6})\s+
    (?P<tid>\d+)\s+
    INFO\s+dfs\.DataNode\$DataXceiver:\s+
    (?:
        (?P<op_receiving>Receiving)\s+block\s+
        (?P<blk_receiving>blk_[0-9\-]+)
        \s+src:\s+/(?P<src_receiving>[0-9.]+):\d+
        \s+dest:\s+/(?P<dst_receiving>[0-9.]+):\d+
        |
        (?P<op_received>Received)\s+block\s+
        (?P<blk_received>blk_[0-9\-]+)
        .*?src:\s+/(?P<src_received>[0-9.]+):\d+
        \s+dest:\s+/(?P<dst_received>[0-9.]+):\d+
        (?:.*?size\s+(?P<size_received>\d+))?
        |
        (?P<src_served>[0-9.]+):\d+\s+
        (?P<op_served>Served)\s+block\s+
        (?P<blk_served>blk_[0-9\-]+)
        \s+to\s+/(?P<dst_served>[0-9.]+)
    )$""",
    re.VERBOSE,
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


def _blk_to_int(value: str) -> int:
    """
    Convert a DataXceiver block id string (blk_...) to an integer.

    :param value: Block id string.
    :return: int
    """
    return int(value.replace("blk_", ""))


def parse_dataxceiver_worker(
    input_path: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    batch_size: int,
) -> None:
    """
    Parse an HDFS DataXceiver log file and insert documents into MongoDB.

    :param input_path: Path to the input log file.
    :param mongo_uri: MongoDB connection URI.
    :param mongo_db: MongoDB database name.
    :param mongo_coll: MongoDB collection name.
    :param batch_size: Maximum batch size for bulk writes.
    :return: None
    """
    client = MongoClient(mongo_uri)
    coll = client[mongo_db][mongo_coll]

    tiny_logger(f"[DATAX] start: {input_path}")

    batch: List[Dict[str, Any]] = []
    total = 0
    matched = 0
    inserted = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        for line_no, raw in enumerate(infile, start=1):
            total += 1
            line = raw.rstrip("\n")

            m = DATAX_REGEX.match(line)
            if not m:
                continue

            matched += 1
            g = m.groupdict()
            ts = ts_hdfs_compact(g["date"], g["time"])

            doc: Optional[Dict[str, Any]] = None

            if g.get("op_receiving"):
                doc = {
                    "logSet": LogType.HDFS_DATAXCEIVER,
                    "actionType": "receiving",
                    "ts": ts,
                    "day": day_str(ts),
                    "sourceIp": g["src_receiving"],
                    "destIp": g["dst_receiving"],
                    "blockId": _blk_to_int(g["blk_receiving"]),
                    "sizeBytes": None,
                    "upvoteCount": 0,
                }
            elif g.get("op_received"):
                size = int(g["size_received"]) if g.get("size_received") else None
                doc = {
                    "logSet": LogType.HDFS_DATAXCEIVER,
                    "actionType": "received",
                    "ts": ts,
                    "day": day_str(ts),
                    "sourceIp": g["src_received"],
                    "destIp": g["dst_received"],
                    "blockId": _blk_to_int(g["blk_received"]),
                    "sizeBytes": size,
                    "upvoteCount": 0,
                }
            elif g.get("op_served"):
                doc = {
                    "logSet": LogType.HDFS_DATAXCEIVER,
                    "actionType": "served",
                    "ts": ts,
                    "day": day_str(ts),
                    "sourceIp": g["src_served"],
                    "destIp": g["dst_served"],
                    "blockId": _blk_to_int(g["blk_served"]),
                    "sizeBytes": None,
                    "upvoteCount": 0,
                }

            if doc is None:
                continue

            doc["ingestKey"] = _ingest_key(input_path, line_no, line)

            batch.append(doc)
            if len(batch) >= batch_size:
                inserted += flush_batch(coll, batch)
                batch = []

    inserted += flush_batch(coll, batch)
    tiny_logger(f"[DATAX] done: matched {matched}/{total}, inserted {inserted}")
