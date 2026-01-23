import re
from typing import Any, Dict, List

from pymongo import MongoClient

from timestamps import ts_apache, day_str
from writer import flush_batch
from util import tiny_logger, LogType

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
    client = MongoClient(mongo_uri)
    coll = client[mongo_db][mongo_coll]

    tiny_logger(f"[ACCESS] start: {input_path}")

    batch: List[Dict[str, Any]] = []
    total = 0
    matched = 0
    inserted = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        for raw_line in infile:
            total += 1
            line = raw_line.rstrip("\n")
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
