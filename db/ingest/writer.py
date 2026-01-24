from typing import Any, Dict, List

from pymongo.collection import Collection
from pymongo.errors import BulkWriteError


def flush_batch(coll: Collection, batch: List[Dict[str, Any]]) -> int:
    """
    Insert a batch of documents into MongoDB.

    Duplicate keys are expected when re-running ingestion. We use unordered
    inserts for throughput and return the number of successful inserts.

    :param coll: Target MongoDB collection.
    :param batch: Documents to insert.
    :return: int
    """
    if not batch:
        return 0

    try:
        res = coll.insert_many(batch, ordered=False)
        return len(res.inserted_ids)
    except BulkWriteError as exc:
        details = exc.details or {}
        inserted = details.get("nInserted")
        if inserted is not None:
            return int(inserted)

        write_errors = details.get("writeErrors", [])
        return max(0, len(batch) - len(write_errors))
