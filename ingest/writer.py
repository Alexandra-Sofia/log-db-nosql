from typing import Any, Dict, List
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

def flush_batch(coll: Collection, batch: List[Dict[str, Any]]) -> int:
    if not batch:
        return 0
    try:
        res = coll.insert_many(batch, ordered=False)
        return len(res.inserted_ids)
    except BulkWriteError as e:
        # With ordered=False you usually still get partial success.
        # We count inserted docs as: attempted - writeErrors
        write_errors = e.details.get("writeErrors", [])
        return max(0, len(batch) - len(write_errors))
