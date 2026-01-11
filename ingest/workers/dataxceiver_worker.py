import re
from typing import Any, Dict, List, Optional

from pymongo import MongoClient

from ingest.timestamps import ts_hdfs_compact, day_str
from ingest.mongo_writer import flush_batch
from ingest.util import tiny_logger, LogType

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

def _blk_to_int(s: str) -> int:
    return int(s.replace("blk_", ""))

def parse_dataxceiver_worker(
    input_path: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    batch_size: int,
) -> None:
    client = MongoClient(mongo_uri)
    coll = client[mongo_db][mongo_coll]

    tiny_logger(f"[DATAX] start: {input_path}")

    batch: List[Dict[str, Any]] = []
    total = 0
    matched = 0
    inserted = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        for raw_line in infile:
            total += 1
            line = raw_line.rstrip("\n")
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

            batch.append(doc)
            if len(batch) >= batch_size:
                inserted += flush_batch(coll, batch)
                batch = []

    inserted += flush_batch(coll, batch)
    tiny_logger(f"[DATAX] done: matched {matched}/{total}, inserted {inserted}")
