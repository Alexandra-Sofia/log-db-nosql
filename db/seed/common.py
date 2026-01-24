# common.py
import os
import random
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from tiny_logger import tiny_logger

"""
Shared utilities for the seeding scripts.

This module centralizes:
    * environment-based configuration loading
    * MongoDB connectivity
    * core index creation
    * shared validation helpers
    * capped random selection for admin allocation
"""


@dataclass(frozen=True)
class MongoConfig:
    """
    MongoDB connection configuration.

    :param mongo_uri: MongoDB connection string.
    :param mongo_db: Target database name.
    :return: MongoConfig
    """

    mongo_uri: str
    mongo_db: str


@dataclass(frozen=True)
class SeedConfig:
    """
    Shared configuration used by multiple seed steps.

    :param mongo: MongoDB configuration.
    :param seed: Random seed for reproducibility.
    :param mode: Seeding mode (fail or topup).
    :return: SeedConfig
    """

    mongo: MongoConfig
    seed: int
    mode: str


@dataclass(frozen=True)
class Collections:
    """
    MongoDB collections used by the project.

    :param logs: Logs collection.
    :param admins: Admins collection.
    :param upvotes: Upvotes collection.
    :return: Collections
    """

    logs: Collection
    admins: Collection
    upvotes: Collection


@dataclass(frozen=True)
class SeedContext:
    """
    Shared context passed to seed steps.

    :param cfg: Shared seed configuration.
    :param db: Mongo database handle.
    :param col: Collection handles.
    :return: SeedContext
    """

    cfg: SeedConfig
    db: Database
    col: Collections


def load_seed_config() -> SeedConfig:
    """
    Load shared seed configuration from environment variables.

    :param None: This function does not accept any parameters.
    :return: SeedConfig
    """
    mongo = MongoConfig(
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.getenv("MONGO_DB", "nosql_logs"),
    )
    return SeedConfig(
        mongo=mongo,
        seed=int(os.getenv("SEED", "42")),
        mode=os.getenv("MODE", "fail"),
    )


def seed_random(cfg: SeedConfig) -> None:
    """
    Seed the global random generator.

    :param cfg: Shared seed configuration.
    :return: None
    """
    random.seed(cfg.seed)


def connect(cfg: SeedConfig) -> SeedContext:
    """
    Connect to MongoDB and build a shared seed context.

    :param cfg: Shared seed configuration.
    :return: SeedContext
    """
    client = MongoClient(cfg.mongo.mongo_uri)
    db = client[cfg.mongo.mongo_db]
    col = Collections(
        logs=db["logs"],
        admins=db["admins"],
        upvotes=db["upvotes"],
    )
    return SeedContext(cfg=cfg, db=db, col=col)


def counts(ctx: SeedContext) -> Tuple[int, int, int]:
    """
    Return document counts for logs, admins, upvotes.

    :param ctx: Shared seed context.
    :return: Tuple[int, int, int]
    """
    L = ctx.col.logs.count_documents({})
    A = ctx.col.admins.count_documents({})
    U = ctx.col.upvotes.count_documents({})
    return L, A, U


def require_non_empty(name: str, count: int, message: str) -> None:
    """
    Validate that a collection has at least one document.

    :param name: Logical collection name used in log output.
    :param count: Document count.
    :param message: Error message if empty.
    :return: None
    """
    if count == 0:
        tiny_logger(f"[SEED][CHECK] {name} is empty.")
        raise SystemExit(message)


def fail_if_exists(mode: str, existing: int, message: str) -> None:
    """
    Enforce MODE semantics for pre-existing data.

    :param mode: Seeding mode (fail or topup).
    :param existing: Existing document count.
    :param message: Error message when mode is fail and data exists.
    :return: None
    """
    if existing > 0 and mode == "fail":
        raise SystemExit(message)


def ensure_core_indexes(ctx: SeedContext) -> None:
    """
    Ensure indexes required for correctness and performance.

    :param ctx: Shared seed context.
    :return: None
    """
    tiny_logger("[SEED][INDEXES] Ensuring core indexes...")

    ctx.col.admins.create_index([("username", 1)], unique=True, name="uniq_username")
    ctx.col.admins.create_index([("email", 1)], name="email")
    ctx.col.upvotes.create_index([("adminId", 1), ("logId", 1)], unique=True, name="uniq_admin_log_vote")
    ctx.col.upvotes.create_index([("usernameUsed", 1), ("logId", 1),("emailUsed", 1)], name="admin_logid_username_used")
    ctx.col.logs.create_index([("day", 1), ("upvoteCount", -1)], name="day_upvotes_desc")
    ctx.col.logs.create_index([("upvoteCount", 1)], name="upvoteCount")

    tiny_logger("[SEED][INDEXES] Core indexes ensured.")


def pick_capped_random_id(ids: list, counters: Dict[Any, int], cap: int) -> Optional[Any]:
    """
    Pick a random id whose counter is below cap.

    :param ids: Candidate ids.
    :param counters: Current counter per id.
    :param cap: Maximum allowed counter value.
    :return: Optional[Any]
    """
    if not ids:
        return None
    for _ in range(100):
        chosen = random.choice(ids)
        if counters.get(chosen, 0) < cap:
            return chosen
    return None
