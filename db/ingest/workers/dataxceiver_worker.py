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


def _blk_to_int(value: str) -> int:
    """
    Convert an HDFS block id string to an integer.

    Format example:
        blk_12345 -> 12345

    :param value: Block id in the form "blk_<digits>".
    :return: int
    """
    return int(value.replace("blk_", ""))


def _make_doc_receiving(groups: Dict[str, Any], ts) -> Dict[str, Any]:
    """
    Build a normalized MongoDB document for a Receiving event.

    :param groups: Regex match group dictionary.
    :param ts: Parsed UTC timestamp.
    :return: Dict[str, Any]
    """
    return {
        "logSet": LogType.HDFS_DATAXCEIVER,
        "actionType": "receiving",
        "ts": ts,
        "day": day_str(ts),
        "sourceIp": groups["src_receiving"],
        "destIp": groups["dst_receiving"],
        "blockId": _blk_to_int(groups["blk_receiving"]),
        "sizeBytes": None,
        "upvoteCount": 0,
    }


def _make_doc_received(groups: Dict[str, Any], ts) -> Dict[str, Any]:
    """
    Build a normalized MongoDB document for a Received event.

    The log line may optionally contain a size field.

    :param groups: Regex match group dictionary.
    :param ts: Parsed UTC timestamp.
    :return: Dict[str, Any]
    """
    size = int(groups["size_received"]) if groups.get("size_received") else None
    return {
        "logSet": LogType.HDFS_DATAXCEIVER,
        "actionType": "received",
        "ts": ts,
        "day": day_str(ts),
        "sourceIp": groups["src_received"],
        "destIp": groups["dst_received"],
        "blockId": _blk_to_int(groups["blk_received"]),
        "sizeBytes": size,
        "upvoteCount": 0,
    }


def _make_doc_served(groups: Dict[str, Any], ts) -> Dict[str, Any]:
    """
    Build a normalized MongoDB document for a Served event.

    :param groups: Regex match group dictionary.
    :param ts: Parsed UTC timestamp.
    :return: Dict[str, Any]
    """
    return {
        "logSet": LogType.HDFS_DATAXCEIVER,
        "actionType": "served",
        "ts": ts,
        "day": day_str(ts),
        "sourceIp": groups["src_served"],
        "destIp": groups["dst_served"],
        "blockId": _blk_to_int(groups["blk_served"]),
        "sizeBytes": None,
        "upvoteCount": 0,
    }


def _build_document(groups: Dict[str, Any], ts) -> Optional[Dict[str, Any]]:
    """
    Build the correct document variant based on which operation matched.

    :param groups: Regex match group dictionary.
    :param ts: Parsed UTC timestamp.
    :return: Optional[Dict[str, Any]]
    """
    if groups.get("op_receiving"):
        return _make_doc_receiving(groups, ts)
    if groups.get("op_received"):
        return _make_doc_received(groups, ts)
    if groups.get("op_served"):
        return _make_doc_served(groups, ts)
    return None


def parse_dataxceiver_worker(
    input_path: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    batch_size: int,
) -> None:
    """
    Parse an HDFS DataXceiver log file and insert entries into MongoDB.

    The function reads the log file line by line, matches DataXceiver patterns,
    normalizes them into a single document schema, and inserts documents into
    MongoDB in batches.

    Lines that do not match the expected DataXceiver patterns are ignored.

    :param input_path: Path to the HDFS DataXceiver log file.
    :param mongo_uri: MongoDB connection URI.
    :param mongo_db: Target MongoDB database name.
    :param mongo_coll: Target MongoDB collection name.
    :param batch_size: Number of documents per bulk insert.
    :return: None
    """
    client = MongoClient(mongo_uri)
    collection = client[mongo_db][mongo_coll]

    tiny_logger(f"[DATAX] start: {input_path}")

    batch: List[Dict[str, Any]] = []
    total_lines = 0
    matched_lines = 0
    inserted_docs = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        for raw_line in infile:
            total_lines += 1
            line = raw_line.rstrip("\n")

            match = DATAX_REGEX.match(line)
            if match is None:
                continue

            matched_lines += 1
            groups = match.groupdict()
            timestamp = ts_hdfs_compact(groups["date"], groups["time"])

            document = _build_document(groups, timestamp)
            if document is None:
                continue

            batch.append(document)

            if len(batch) >= batch_size:
                inserted_docs += flush_batch(collection, batch)
                batch.clear()

    inserted_docs += flush_batch(collection, batch)

    tiny_logger(
        f"[DATAX] done: matched {matched_lines}/{total_lines}, "
        f"inserted {inserted_docs}"
    )
