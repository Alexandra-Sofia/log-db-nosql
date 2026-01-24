import re
from typing import Any, Dict, List, Optional

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


def _parse_namesys_update(line: str) -> Optional[Dict[str, Any]]:
    """
    Parse a single FSNamesystem "blockMap updated" line into a document.

    :param line: Log line string.
    :return: Optional[Dict[str, Any]]
    """
    match = NAMESYS_UPDATE_REGEX.match(line)
    if match is None:
        return None

    groups = match.groupdict()
    timestamp = ts_hdfs_compact(groups["date"], groups["time"])
    size = int(groups["size"]) if groups.get("size") else None

    return {
        "logSet": LogType.HDFS_NAMESYSTEM,
        "actionType": "update",
        "ts": timestamp,
        "day": day_str(timestamp),
        "sourceIp": None,
        "destIp": groups["ip"],
        "blockId": int(groups["block"]),
        "sizeBytes": size,
        "upvoteCount": 0,
    }


def _parse_namesys_replicate(line: str) -> Optional[List[Dict[str, Any]]]:
    """
    Parse a single FSNamesystem "ask ... to replicate" line into documents.

    One document is produced per destination datanode.

    :param line: Log line string.
    :return: Optional[List[Dict[str, Any]]]
    """
    match = NAMESYS_ASK_REPLICATE_REGEX.match(line)
    if match is None:
        return None

    groups = match.groupdict()
    timestamp = ts_hdfs_compact(groups["date"], groups["time"])
    src_ip = groups["src_ip"]
    block_id = int(groups["block"])

    docs: List[Dict[str, Any]] = []
    for token in groups["dest_list"].split():
        if ":" not in token:
            continue
        dest_ip = token.split(":", 1)[0]

        docs.append(
            {
                "logSet": LogType.HDFS_NAMESYSTEM,
                "actionType": "replicate",
                "ts": timestamp,
                "day": day_str(timestamp),
                "sourceIp": src_ip,
                "destIp": dest_ip,
                "blockId": block_id,
                "sizeBytes": None,
                "upvoteCount": 0,
            }
        )

    return docs


def parse_namesystem_worker(
    input_path: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    batch_size: int,
) -> None:
    """
    Parse an HDFS FSNamesystem log file and insert entries into MongoDB.

    The function reads the log file line by line and applies two patterns:
        * blockMap updated events
        * ask-to-replicate events (expanded to one doc per destination)

    Documents are inserted into MongoDB in batches.

    :param input_path: Path to the HDFS FSNamesystem log file.
    :param mongo_uri: MongoDB connection URI.
    :param mongo_db: Target MongoDB database name.
    :param mongo_coll: Target MongoDB collection name.
    :param batch_size: Number of documents per bulk insert.
    :return: None
    """
    client = MongoClient(mongo_uri)
    collection = client[mongo_db][mongo_coll]

    tiny_logger(f"[NAMESYS] start: {input_path}")

    batch: List[Dict[str, Any]] = []
    total_lines = 0
    matched_lines = 0
    inserted_docs = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        for raw_line in infile:
            total_lines += 1
            line = raw_line.strip()

            doc_update = _parse_namesys_update(line)
            if doc_update is not None:
                matched_lines += 1
                batch.append(doc_update)

                if len(batch) >= batch_size:
                    inserted_docs += flush_batch(collection, batch)
                    batch.clear()

                continue

            docs_repl = _parse_namesys_replicate(line)
            if docs_repl is not None:
                matched_lines += 1

                for doc in docs_repl:
                    batch.append(doc)

                    if len(batch) >= batch_size:
                        inserted_docs += flush_batch(collection, batch)
                        batch.clear()

    inserted_docs += flush_batch(collection, batch)

    tiny_logger(
        f"[NAMESYS] done: matched {matched_lines}/{total_lines}, "
        f"inserted {inserted_docs}"
    )
