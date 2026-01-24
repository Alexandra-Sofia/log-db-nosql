from typing import Any, Dict, List

from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

"""
Batch insert utilities.

This module provides a helper for safely inserting batches of documents into
MongoDB using unordered bulk inserts, while accounting for partial successes.
"""


def flush_batch(coll: Collection, batch: List[Dict[str, Any]]) -> int:
    """
    Insert a batch of documents into a MongoDB collection.

    The insert is performed using an unordered bulk operation to maximize
    throughput. In case of partial failures, the function returns the number
    of successfully inserted documents.

    :param coll: Target MongoDB collection.
    :param batch: List of documents to insert.
    :return: int
    """
    if not batch:
        return 0

    try:
        result = coll.insert_many(batch, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as exc:
        write_errors = exc.details.get("writeErrors", [])
        return max(0, len(batch) - len(write_errors))
