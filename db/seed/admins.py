# admins.py
import os
from dataclasses import dataclass
from typing import Dict, List

from faker import Faker

from common import SeedContext
from tiny_logger import tiny_logger

"""
Administrator seeding step.

This module generates the administrator dataset using Faker and populates the
``admins`` collection. The step is idempotent: if admins already exist, it
logs and exits without modifying the collection.
"""


@dataclass(frozen=True)
class AdminsConfig:
    """
    Configuration for administrator generation.

    :param n_admins: Number of administrators to generate.
    :return: AdminsConfig
    """

    n_admins: int


def load_admins_config() -> AdminsConfig:
    """
    Load administrator seeding configuration from environment variables.

    :param None: This function does not accept any parameters.
    :return: AdminsConfig
    """
    return AdminsConfig(n_admins=int(os.getenv("N_ADMINS", "500")))


def admins_exist(ctx: SeedContext) -> int:
    """
    Return the number of existing administrators.

    :param ctx: Shared seed context.
    :return: int
    """
    return ctx.col.admins.count_documents({})


def build_admin_docs(fake: Faker, n_admins: int) -> List[Dict]:
    """
    Build administrator documents.

    :param fake: Faker instance used for data generation.
    :param n_admins: Number of admin documents to generate.
    :return: List[Dict]
    """
    docs: List[Dict] = []
    for _ in range(n_admins):
        docs.append(
            {
                "username": fake.unique.user_name(),
                "email": fake.unique.email(),
                "phone": fake.phone_number(),
                "totalUpvotes": 0,
            }
        )
    return docs


def seed_admins(ctx: SeedContext, cfg: AdminsConfig) -> int:
    """
    Seed administrators if none exist.

    :param ctx: Shared seed context.
    :param cfg: Administrator seeding configuration.
    :return: int
    """
    existing = admins_exist(ctx)
    if existing > 0:
        tiny_logger(f"[SEED][ADMINS] Admins already exist ({existing}). Skipping.")
        return 0

    tiny_logger(f"[SEED][ADMINS] Generating {cfg.n_admins} administrators...")

    fake = Faker()
    docs = build_admin_docs(fake, cfg.n_admins)

    res = ctx.col.admins.insert_many(docs)
    inserted = len(res.inserted_ids)

    tiny_logger(f"[SEED][ADMINS] Inserted {inserted} admins.")
    return inserted
