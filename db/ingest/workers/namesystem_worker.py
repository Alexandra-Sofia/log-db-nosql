import hashlib
import re
from typing import Any, Dict, List

from pymongo import MongoClient

from timestamps import ts_hdfs_compact, day_str
from writer import flush_batch
from util import tiny_logger, LogType

"""
HDFS FSNamesystem ingestion worker.

This module parses HDFS NameNode FSNamesystem INFO log lines and normalizes two
event families into MongoDB documents:

    * BLOCK* NameSystem.<op>: blockMap updated: <ip>:<port> ... blk_<id> [size <n>]
    * BLOCK* ask <src>:<port> to replicate blk_<id> to datanode(s) <dest_list>

The replicate pattern expands into one document per destination datanode.
Lines that do not match any supported pattern are ignored.
"""

NAMESYS_UPDATE_REGEX = re.compile(
    r"""
    ^(?P<date>\d{6})\s+
    (?P<time>\d{6})\s+
    (?P<tid>\d+)\s+
    INFO\s+dfs\.FSNamesystem:\s+BLOCK\*\s+
    NameSystem\.\w+:\s+
    blockMap\s+updated:\s+
    (?P<ip>[0-9.]+):\d+.*?
    blk_(?P<block>-?\d+)
    (?:\s+size\s+(?P<size>\d+))?
    \s*$
    """,
    re.VERBOSE,
)

NAMESYS_ASK_REPLICATE_REGEX = re.compile(
    r"""
    ^(?P<date>\d{6})\s+
    (?P<time>\d{6})\s+
    (?P<tid>\d+)\s+
    INFO\s+dfs\.FSNamesystem:\s+BLOCK\*\s+
    ask\s+(?P<src_ip>[0-9.]+):\d+
    \s+to\s+replicate\s+
    blk_(?P<block>-?\d+)
    \s+to\s+datanode\(s\)\s+
    (?P<dest_list>(?:[0-9.]+:\d+\s*)+)
    \s*$
    """,
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


def _ingest_key_with_suffix(input_path: str, line_no: int, raw_line: str, suffix: str) -> str:
    """
    Build a deterministic ingestion key with a suffix for fan-out lines.

    :param input_path: Input file path.
    :param line_no: 1-based line number.
    :param raw_line: Raw line content (without trailing newline).
    :param suffix: Extra differentiator (e.g., dest_ip).
    :return: str
    """
    material = f"{input_path}|{line_no}|{raw_line}|{suffix}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def parse_namesystem_worker(
    input_path: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    batch_size: int,
) -> None:
    """
    Parse an HDFS FSNamesystem log file and insert documents into MongoDB.

    :param input_path: Path to the input log file.
    :param mongo_uri: MongoDB connection URI.
    :param mongo_db: MongoDB database name.
    :param mongo_coll: MongoDB collection name.
    :param batch_size: Maximum batch size for bulk writes.
    :return: None
    """
    client = MongoClient(mongo_uri)
    coll = client[mongo_db][mongo_coll]

    tiny_logger(f"[NAMESYS] start: {input_path}")

    batch: List[Dict[str, Any]] = []
    total = 0
    matched = 0
    inserted = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        for line_no, raw in enumerate(infile, start=1):
            total += 1
            line = raw.strip()

            m_upd = NAMESYS_UPDATE_REGEX.match(line)
            if m_upd:
                matched += 1
                g = m_upd.groupdict()
                ts = ts_hdfs_compact(g["date"], g["time"])
                size = int(g["size"]) if g.get("size") else None

                doc = {
                    "logSet": LogType.HDFS_NAMESYSTEM,
                    "ingestKey": _ingest_key(input_path, line_no, line),
                    "actionType": "update",
                    "ts": ts,
                    "day": day_str(ts),
                    "sourceIp": None,
                    "destIp": g["ip"],
                    "blockId": int(g["block"]),
                    "sizeBytes": size,
                    "upvoteCount": 0,
                }

                batch.append(doc)
                if len(batch) >= batch_size:
                    inserted += flush_batch(coll, batch)
                    batch = []
                continue

            m_rep = NAMESYS_ASK_REPLICATE_REGEX.match(line)
            if m_rep:
                matched += 1
                g = m_rep.groupdict()
                ts = ts_hdfs_compact(g["date"], g["time"])
                src_ip = g["src_ip"]
                block_id = int(g["block"])

                for token in g["dest_list"].split():
                    if ":" not in token:
                        continue
                    dest_ip = token.split(":", 1)[0]

                    doc = {
                        "logSet": LogType.HDFS_NAMESYSTEM,
                        "ingestKey": _ingest_key_with_suffix(input_path, line_no, line, dest_ip),
                        "actionType": "replicate",
                        "ts": ts,
                        "day": day_str(ts),
                        "sourceIp": src_ip,
                        "destIp": dest_ip,
                        "blockId": block_id,
                        "sizeBytes": None,
                        "upvoteCount": 0,
                    }

                    batch.append(doc)
                    if len(batch) >= batch_size:
                        inserted += flush_batch(coll, batch)
                        batch = []

    inserted += flush_batch(coll, batch)
    tiny_logger(f"[NAMESYS] done: matched {matched}/{total}, inserted {inserted}")
