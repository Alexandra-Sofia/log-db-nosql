import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from common import SeedContext, pick_capped_random_id
from tiny_logger import tiny_logger

"""
Upvote seeding step.

Optimizations:
    * coverage uses insert_many batching
    * no per-vote updates to logs/admins counters
    * extra vote phase is bounded and capacity-aware
    * counter recompute does not do full-collection resets

Constraints enforced:
    * at least one third of logs have at least one upvote
    * no administrator has more than ADMIN_CAP upvotes
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


def _safe_ninserted(exc: BulkWriteError) -> int:
    """
    Extract inserted count from a BulkWriteError.

    :param exc: BulkWriteError raised by PyMongo.
    :return: int
    """
    details = getattr(exc, "details", {}) or {}
    return int(details.get("nInserted", 0))


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
        return _safe_ninserted(exc)


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
    Compute the desired total number of votes.

    :param log_count: Total logs.
    :param extra_vote_frac: Extra vote fraction.
    :return: int
    """
    return target_covered(log_count) + int(math.ceil(extra_vote_frac * log_count))


def make_vote(now: datetime, aid: Any, admin: Dict[str, Any], log_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a vote document.

    :param now: UTC timestamp for the vote.
    :param aid: Administrator id.
    :param admin: Administrator document.
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


def recompute_counters(ctx: SeedContext) -> None:
    """
    Recompute logs.upvoteCount and admins.totalUpvotes from upvotes.

    This does not reset all counters to 0. It overwrites counters for ids that appear in upvotes.
    For the project constraints, this is sufficient and avoids two full-collection scans.

    :param ctx: Shared seed context.
    :return: None
    """
    tiny_logger("[SEED][UPVOTES] Recomputing counters from upvotes collection...")

    log_updates: List[UpdateOne] = []
    for row in ctx.col.upvotes.aggregate([{"$group": {"_id": "$logId", "cnt": {"$sum": 1}}}]):
        log_updates.append(UpdateOne({"_id": row["_id"]}, {"$set": {"upvoteCount": row["cnt"]}}))

    if log_updates:
        res = ctx.col.logs.bulk_write(log_updates, ordered=False)
        tiny_logger(f"[SEED][UPVOTES] Updated logs.upvoteCount for {res.modified_count} logs.")
    else:
        tiny_logger("[SEED][UPVOTES] No log counters to update (no upvotes yet).")

    admin_updates: List[UpdateOne] = []
    for row in ctx.col.upvotes.aggregate([{"$group": {"_id": "$adminId", "cnt": {"$sum": 1}}}]):
        admin_updates.append(UpdateOne({"_id": row["_id"]}, {"$set": {"totalUpvotes": row["cnt"]}}))

    if admin_updates:
        res = ctx.col.admins.bulk_write(admin_updates, ordered=False)
        tiny_logger(f"[SEED][UPVOTES] Updated admins.totalUpvotes for {res.modified_count} admins.")
    else:
        tiny_logger("[SEED][UPVOTES] No admin counters to update (no upvotes yet).")


def ensure_coverage(
    ctx: SeedContext,
    cfg: UpvotesConfig,
    need: int,
    admin_ids: List[Any],
    admin_by_id: Dict[Any, Dict[str, Any]],
    admin_counts: Dict[Any, int],
    progress_every: int,
    insert_batch_size: int,
    cursor_batch_size: int,
) -> int:
    """
    Ensure coverage by inserting votes for logs with upvoteCount == 0.

    :param ctx: Shared seed context.
    :param cfg: Upvotes configuration.
    :param need: Required additional covered logs.
    :param admin_ids: Admin id list.
    :param admin_by_id: Map admin id to admin doc.
    :param admin_counts: Map admin id to current upvote count.
    :param progress_every: Progress log frequency.
    :param insert_batch_size: insert_many batch size.
    :param cursor_batch_size: Mongo cursor batch size.
    :return: int
    """
    if need <= 0:
        tiny_logger("[SEED][UPVOTES] Coverage already satisfied; no mandatory votes needed.")
        return 0

    tiny_logger(f"[SEED][UPVOTES] Ensuring coverage: inserting at least {need} new votes on 0-vote logs...")

    now = datetime.now(timezone.utc)
    inserted = 0
    batch: List[Dict[str, Any]] = []

    cursor = ctx.col.logs.find(
        {"upvoteCount": 0},
        {"_id": 1, "day": 1, "sourceIp": 1, "blockId": 1},
        batch_size=cursor_batch_size,
        no_cursor_timeout=True,
    ).limit(need)

    try:
        for log_doc in cursor:
            aid = pick_capped_random_id(admin_ids, admin_counts, cfg.admin_cap)
            if aid is None:
                raise SystemExit("Cannot allocate more votes without exceeding admin cap.")

            batch.append(make_vote(now, aid, admin_by_id[aid], log_doc))
            admin_counts[aid] += 1

            if len(batch) >= insert_batch_size:
                inserted += insert_many(ctx.col.upvotes, batch)
                batch = []

                if progress_every > 0 and inserted % progress_every == 0:
                    tiny_logger(f"[SEED][UPVOTES] Coverage progress: {inserted}/{need}")

        if batch:
            inserted += insert_many(ctx.col.upvotes, batch)

    finally:
        cursor.close()

    tiny_logger(f"[SEED][UPVOTES] Coverage step complete: inserted {inserted} votes.")
    return inserted


def _remaining_capacity(admin_counts: Dict[Any, int], cap: int) -> int:
    """
    Compute remaining capacity across admins before reaching cap.

    :param admin_counts: Map admin id to current upvote count.
    :param cap: Maximum upvotes per administrator.
    :return: int
    """
    return sum(max(0, cap - v) for v in admin_counts.values())


def add_extra_votes(
    ctx: SeedContext,
    cfg: UpvotesConfig,
    remaining: int,
    admin_ids: List[Any],
    admin_by_id: Dict[Any, Dict[str, Any]],
    admin_counts: Dict[Any, int],
    log_count: int,
    insert_batch_size: int,
) -> int:
    """
    Add extra votes for richer distributions.

    This phase is strictly bounded by remaining admin capacity and by a capped sampling pool.

    :param ctx: Shared seed context.
    :param cfg: Upvotes configuration.
    :param remaining: Number of extra votes to attempt.
    :param admin_ids: Admin id list.
    :param admin_by_id: Map admin id to admin doc.
    :param admin_counts: Map admin id to current upvote count.
    :param log_count: Total logs count.
    :param insert_batch_size: insert_many batch size.
    :return: int
    """
    if remaining <= 0:
        tiny_logger("[SEED][UPVOTES] No extra votes needed (already at/above target_total_votes).")
        return 0

    capacity_left = _remaining_capacity(admin_counts, cfg.admin_cap)
    if capacity_left <= 0:
        tiny_logger("[SEED][UPVOTES] No extra votes possible (admin capacity exhausted).")
        return 0

    remaining = min(remaining, capacity_left)
    tiny_logger(f"[SEED][UPVOTES] Adding extra votes for richness: remaining={remaining}")

    max_pool = int(os.getenv("EXTRA_POOL_MAX", "200000"))
    pool_size = min(log_count, max_pool)
    tiny_logger(f"[SEED][UPVOTES] Building sampling pool of logs (pool_size={pool_size})...")
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

        if len(batch) >= insert_batch_size:
            inserted_total += insert_many(ctx.col.upvotes, batch)
            batch = []

    inserted_total += insert_many(ctx.col.upvotes, batch)
    tiny_logger("[SEED][UPVOTES] Extra vote step complete.")
    return inserted_total


def validate_constraints(ctx: SeedContext, cfg: UpvotesConfig, required_covered: int) -> Tuple[int, int]:
    """
    Validate coverage and cap constraints.

    :param ctx: Shared seed context.
    :param cfg: Upvotes configuration.
    :param required_covered: Minimum covered logs required.
    :return: Tuple[int, int]
    """
    covered = ctx.col.logs.count_documents({"upvoteCount": {"$gte": 1}})
    max_admin_cursor = ctx.col.admins.find({}, {"totalUpvotes": 1}).sort("totalUpvotes", -1).limit(1)
    max_admin_val = int(next(max_admin_cursor, {}).get("totalUpvotes", 0))

    if covered < required_covered:
        raise SystemExit("Constraint failed: fewer than 1/3 logs have upvotes.")
    if max_admin_val > cfg.admin_cap:
        raise SystemExit("Constraint failed: an admin exceeded 1000 upvotes.")

    return covered, max_admin_val


def seed_upvotes(ctx: SeedContext, cfg: UpvotesConfig) -> int:
    """
    Seed upvotes according to project constraints.

    :param ctx: Shared seed context.
    :param cfg: Upvotes configuration.
    :return: int
    """
    tiny_logger("[SEED][UPVOTES] START")

    L = ctx.col.logs.count_documents({})
    A = ctx.col.admins.count_documents({})
    U = ctx.col.upvotes.count_documents({})

    required = target_covered(L)
    capacity = A * cfg.admin_cap
    if capacity < required:
        min_admins = int(math.ceil(required / cfg.admin_cap))
        raise SystemExit(
            f"Insufficient capacity for coverage: need {required} covered logs, "
            f"but only {A} admins with cap {cfg.admin_cap} gives capacity {capacity}. "
            f"Increase N_ADMINS to at least {min_admins}."
        )

    covered_now = ctx.col.logs.count_documents({"upvoteCount": {"$gte": 1}})
    need = max(0, required - covered_now)

    desired_total = target_total_votes(L, cfg.extra_vote_frac)

    tiny_logger(f"[SEED][UPVOTES] Logs: {L}")
    tiny_logger(f"[SEED][UPVOTES] Admins: {A}")
    tiny_logger(f"[SEED][UPVOTES] Existing upvotes: {U}")
    tiny_logger(f"[SEED][UPVOTES] Covered logs now: {covered_now} (need at least {required})")
    tiny_logger(f"[SEED][UPVOTES] Need additional covered logs: {need}")
    tiny_logger(f"[SEED][UPVOTES] Target total votes: {desired_total} (EXTRA_VOTE_FRAC={cfg.extra_vote_frac})")

    admin_ids, admin_by_id, admin_counts = load_admin_state(ctx)

    progress_every = int(os.getenv("COVERAGE_PROGRESS_EVERY", "100000"))
    insert_batch_size = int(os.getenv("VOTE_INSERT_BATCH_SIZE", "10000"))
    cursor_batch_size = int(os.getenv("COVERAGE_CURSOR_BATCH_SIZE", "50000"))

    inserted_coverage = ensure_coverage(
        ctx=ctx,
        cfg=cfg,
        need=need,
        admin_ids=admin_ids,
        admin_by_id=admin_by_id,
        admin_counts=admin_counts,
        progress_every=progress_every,
        insert_batch_size=insert_batch_size,
        cursor_batch_size=cursor_batch_size,
    )

    current_upvotes = U + inserted_coverage
    remaining = max(0, desired_total - current_upvotes)

    inserted_extra = add_extra_votes(
        ctx=ctx,
        cfg=cfg,
        remaining=remaining,
        admin_ids=admin_ids,
        admin_by_id=admin_by_id,
        admin_counts=admin_counts,
        log_count=L,
        insert_batch_size=insert_batch_size,
    )

    recompute_counters(ctx)
    covered, max_admin_val = validate_constraints(ctx, cfg, required)

    inserted_total = inserted_coverage + inserted_extra
    tiny_logger(f"[SEED][UPVOTES] Inserted votes this run: {inserted_total}")
    tiny_logger(f"[SEED][UPVOTES] Logs with >=1 upvote: {covered} (required >= {required})")
    tiny_logger(f"[SEED][UPVOTES] Max admin totalUpvotes: {max_admin_val} (cap {cfg.admin_cap})")
    tiny_logger("[SEED][UPVOTES] END")

    return inserted_total
