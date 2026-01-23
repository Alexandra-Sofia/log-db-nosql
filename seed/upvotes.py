import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, DuplicateKeyError

from common import SeedContext, pick_capped_random_id
from tiny_logger import tiny_logger

"""
Upvote seeding step.

This module contains upvote-specific configuration and logic.
Mongo connectivity, indexes, and shared validations are handled centrally.
"""


@dataclass(frozen=True)
class UpvotesConfig:
    """
    Configuration for upvote generation.

    :param extra_vote_frac: Fraction of extra votes relative to total logs.
    :param admin_cap: Maximum upvotes per administrator.
    :return: UpvotesConfig
    """

    extra_vote_frac: float
    admin_cap: int


def load_upvotes_config() -> UpvotesConfig:
    """
    Load upvote seed configuration from environment variables.

    :param None: This function does not accept any parameters.
    :return: UpvotesConfig
    """
    return UpvotesConfig(
        extra_vote_frac=float(os.getenv("EXTRA_VOTE_FRAC", "0.20")),
        admin_cap=int(os.getenv("ADMIN_CAP", "1000")),
    )


def recompute_counters(ctx: SeedContext) -> None:
    """
    Recompute logs.upvoteCount and admins.totalUpvotes from upvotes.

    :param ctx: Shared seed context.
    :return: None
    """
    tiny_logger("[SEED][UPVOTES] Recomputing counters from upvotes collection...")

    log_updates: List[UpdateOne] = []
    for row in ctx.col.upvotes.aggregate([{"$group": {"_id": "$logId", "cnt": {"$sum": 1}}}]):
        log_updates.append(UpdateOne({"_id": row["_id"]}, {"$set": {"upvoteCount": row["cnt"]}}))
    if log_updates:
        ctx.col.logs.bulk_write(log_updates, ordered=False)

    admin_updates: List[UpdateOne] = []
    for row in ctx.col.upvotes.aggregate([{"$group": {"_id": "$adminId", "cnt": {"$sum": 1}}}]):
        admin_updates.append(UpdateOne({"_id": row["_id"]}, {"$set": {"totalUpvotes": row["cnt"]}}))
    if admin_updates:
        ctx.col.admins.bulk_write(admin_updates, ordered=False)


def load_admin_state(ctx: SeedContext) -> Tuple[List[Any], Dict[Any, Dict[str, Any]], Dict[Any, int]]:
    """
    Load admins and cached totalUpvotes into in-memory structures.

    :param ctx: Shared seed context.
    :return: Tuple[List[Any], Dict[Any, Dict[str, Any]], Dict[Any, int]]
    """
    docs = list(ctx.col.admins.find({}, {"_id": 1, "email": 1, "username": 1, "totalUpvotes": 1}))
    ids = [d["_id"] for d in docs]
    by_id = {d["_id"]: d for d in docs}
    counts = {d["_id"]: int(d.get("totalUpvotes", 0) or 0) for d in docs}
    return ids, by_id, counts


def target_covered(log_count: int) -> int:
    """
    Compute the required number of covered logs.

    :param log_count: Total logs.
    :return: int
    """
    return int(math.ceil(log_count / 3))


def target_total_votes(log_count: int, extra_vote_frac: float) -> int:
    """
    Compute the total desired votes.

    :param log_count: Total logs.
    :param extra_vote_frac: Fraction of extra votes relative to total logs.
    :return: int
    """
    return target_covered(log_count) + int(math.ceil(extra_vote_frac * log_count))


def make_vote(now: datetime, aid: Any, admin: Dict[str, Any], log_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a vote document.

    :param now: UTC timestamp for the vote.
    :param aid: Admin id.
    :param admin: Admin document.
    :param log_doc: Log document.
    :return: Dict[str, Any]
    """
    block_ids: List[int] = []
    if isinstance(log_doc.get("blockId"), int):
        block_ids.append(log_doc["blockId"])

    return {
        "adminId": aid,
        "logId": log_doc["_id"],
        "ts": now,
        "day": log_doc.get("day"),
        "emailUsed": admin.get("email"),
        "usernameUsed": admin.get("username"),
        "sourceIp": log_doc.get("sourceIp"),
        "blockIds": block_ids,
    }


def insert_vote_and_bump(ctx: SeedContext, vote_doc: Dict[str, Any]) -> None:
    """
    Insert a vote and increment cached counters.

    :param ctx: Shared seed context.
    :param vote_doc: Vote document to insert.
    :return: None
    """
    ctx.col.upvotes.insert_one(vote_doc)
    ctx.col.logs.update_one({"_id": vote_doc["logId"]}, {"$inc": {"upvoteCount": 1}})
    ctx.col.admins.update_one({"_id": vote_doc["adminId"]}, {"$inc": {"totalUpvotes": 1}})


def ensure_coverage(
    ctx: SeedContext,
    cfg: UpvotesConfig,
    need: int,
    admin_ids: List[Any],
    admin_by_id: Dict[Any, Dict[str, Any]],
    admin_counts: Dict[Any, int],
) -> int:
    """
    Ensure coverage by upvoting logs with upvoteCount == 0.

    :param ctx: Shared seed context.
    :param cfg: Upvotes configuration.
    :param need: Required additional covered logs.
    :param admin_ids: Admin id list.
    :param admin_by_id: Map admin id to admin doc.
    :param admin_counts: Map admin id to current upvote count.
    :return: int
    """
    if need <= 0:
        return 0

    now = datetime.now(timezone.utc)
    inserted = 0
    cursor = ctx.col.logs.find({"upvoteCount": 0}, {"_id": 1, "day": 1, "sourceIp": 1, "blockId": 1}).limit(need)

    for log_doc in cursor:
        aid = pick_capped_random_id(admin_ids, admin_counts, cfg.admin_cap)
        if aid is None:
            raise SystemExit("Cannot allocate more votes without exceeding admin cap.")
        try:
            insert_vote_and_bump(ctx, make_vote(now, aid, admin_by_id[aid], log_doc))
            admin_counts[aid] += 1
            inserted += 1
        except DuplicateKeyError:
            aid2 = pick_capped_random_id(admin_ids, admin_counts, cfg.admin_cap)
            if aid2 is None:
                raise SystemExit("Cannot allocate more votes without exceeding admin cap.")
            insert_vote_and_bump(ctx, make_vote(now, aid2, admin_by_id[aid2], log_doc))
            admin_counts[aid2] += 1
            inserted += 1

    return inserted


def insert_many(upvotes_col, batch: List[Dict[str, Any]]) -> int:
    """
    Insert a batch of votes.

    :param upvotes_col: Upvotes collection handle.
    :param batch: Vote documents.
    :return: int
    """
    if not batch:
        return 0
    try:
        res = upvotes_col.insert_many(batch, ordered=False)
        return len(res.inserted_ids)
    except BulkWriteError as exc:
        return int(exc.details.get("nInserted", 0))


def add_extra_votes(
    ctx: SeedContext,
    cfg: UpvotesConfig,
    remaining: int,
    admin_ids: List[Any],
    admin_by_id: Dict[Any, Dict[str, Any]],
    admin_counts: Dict[Any, int],
    log_count: int,
) -> int:
    """
    Add extra votes for richer distributions.

    :param ctx: Shared seed context.
    :param cfg: Upvotes configuration.
    :param remaining: Number of extra votes to attempt.
    :param admin_ids: Admin id list.
    :param admin_by_id: Map admin id to admin doc.
    :param admin_counts: Map admin id to current upvote count.
    :param log_count: Total logs count.
    :return: int
    """
    if remaining <= 0:
        return 0

    pool_size = min(log_count, max(20000, remaining * 3))
    sample_logs = list(ctx.col.logs.find({}, {"_id": 1, "day": 1, "sourceIp": 1, "blockId": 1}).limit(pool_size))

    now = datetime.now(timezone.utc)
    batch: List[Dict[str, Any]] = []
    inserted_total = 0

    for _ in range(remaining):
        aid = pick_capped_random_id(admin_ids, admin_counts, cfg.admin_cap)
        if aid is None:
            break
        log_doc = random.choice(sample_logs)
        batch.append(make_vote(now, aid, admin_by_id[aid], log_doc))
        admin_counts[aid] += 1
        if len(batch) >= 5000:
            inserted_total += insert_many(ctx.col.upvotes, batch)
            batch = []

    inserted_total += insert_many(ctx.col.upvotes, batch)
    return inserted_total


def validate_constraints(ctx: SeedContext, cfg: UpvotesConfig, required_covered: int) -> None:
    """
    Validate coverage and cap constraints.

    :param ctx: Shared seed context.
    :param cfg: Upvotes configuration.
    :param required_covered: Minimum covered logs required.
    :return: None
    """
    covered = ctx.col.logs.count_documents({"upvoteCount": {"$gte": 1}})
    max_admin_cursor = ctx.col.admins.find({}, {"totalUpvotes": 1}).sort("totalUpvotes", -1).limit(1)
    max_admin_val = next(max_admin_cursor, {}).get("totalUpvotes", 0)

    if covered < required_covered:
        raise SystemExit("Constraint failed: fewer than 1/3 logs have upvotes.")
    if max_admin_val > cfg.admin_cap:
        raise SystemExit("Constraint failed: an admin exceeded 1000 upvotes.")


def seed_upvotes(ctx: SeedContext, cfg: UpvotesConfig) -> int:
    """
    Seed upvotes according to project constraints.

    :param ctx: Shared seed context.
    :param cfg: Upvotes configuration.
    :return: int
    """
    recompute_counters(ctx)

    L = ctx.col.logs.count_documents({})
    required = target_covered(L)
    covered_now = ctx.col.logs.count_documents({"upvoteCount": {"$gte": 1}})
    need = max(0, required - covered_now)

    desired_total = target_total_votes(L, cfg.extra_vote_frac)
    current_votes = ctx.col.upvotes.count_documents({})
    remaining = max(0, desired_total - current_votes)

    admin_ids, admin_by_id, admin_counts = load_admin_state(ctx)

    inserted_coverage = ensure_coverage(ctx, cfg, need, admin_ids, admin_by_id, admin_counts)
    inserted_extra = add_extra_votes(ctx, cfg, remaining, admin_ids, admin_by_id, admin_counts, L)

    recompute_counters(ctx)
    validate_constraints(ctx, cfg, required)

    return inserted_coverage + inserted_extra
