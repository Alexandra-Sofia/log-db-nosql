import re
from typing import Any, Dict, List

from pymongo import MongoClient

from ingest.timestamps import ts_hdfs_compact, day_str
from ingest.mongo_writer import flush_batch
from ingest.util import tiny_logger, LogType

NAMESYS_UPDATE_REGEX = re.compile(
    r"""
    ^(?P<date>\d{6})\s+
    (?P<time>\d{6})\s+
    (?P<tid>\d+)\s+
    INFO\s+dfs\.FSNamesystem:\s+BLOCK\*\s+
    NameSystem\.\w+:\s+
    blockMap updated:\s+
    (?P<ip>[0-9.]+):\d+.*?
    blk_(?P<block>-?\d+)
    (?:\s+size\s+(?P<size>\d+))?
    $""",
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
    $""",
    re.VERBOSE,
)

def parse_namesystem_worker(
    input_path: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll: str,
    batch_size: int,
) -> None:
    client = MongoClient(mongo_uri)
    coll = client[mongo_db][mongo_coll]

    tiny_logger(f"[NAMESYS] start: {input_path}")

    batch: List[Dict[str, Any]] = []
    total = 0
    matched = 0
    inserted = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        for raw_line in infile:
            total += 1
            line = raw_line.rstrip("\n")

            m_upd = NAMESYS_UPDATE_REGEX.match(line)
            if m_upd:
                matched += 1
                g = m_upd.groupdict()
                ts = ts_hdfs_compact(g["date"], g["time"])
                size = int(g["size"]) if g.get("size") else None

                doc = {
                    "logSet": LogType.HDFS_NAMESYSTEM,
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
