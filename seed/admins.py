import os
from dataclasses import dataclass
from typing import Dict, List

from faker import Faker

from common import SeedContext
from tiny_logger import tiny_logger

"""
Administrator seeding step.

This module only contains admin-specific configuration and logic.
Mongo connectivity, indexes, and shared validations are handled centrally.
"""


@dataclass(frozen=True)
class AdminsConfig:
    """
    Configuration for administrator generation.

    :param n_admins: Number of admins to generate.
    :return: AdminsConfig
    """

    n_admins: int


def load_admins_config() -> AdminsConfig:
    """
    Load admin seed configuration from environment variables.

    :param None: This function does not accept any parameters.
    :return: AdminsConfig
    """
    return AdminsConfig(n_admins=int(os.getenv("N_ADMINS", "500")))


def admins_exist(ctx: SeedContext) -> int:
    """
    Return existing admin document count.

    :param ctx: Shared seed context.
    :return: int
    """
    return ctx.col.admins.count_documents({})


def build_admin_docs(fake: Faker, n_admins: int) -> List[Dict]:
    """
    Build administrator documents.

    :param fake: Faker instance.
    :param n_admins: Number of admins to generate.
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
    :param cfg: Admin seeding configuration.
    :return: int
    """
    existing = admins_exist(ctx)
    if existing > 0:
        tiny_logger(f"[SEED][ADMINS] Admins already exist ({existing}). Skipping.")
        return 0

    tiny_logger(f"[SEED][ADMINS] Generating {cfg.n_admins} administrators...")
    fake = Faker()
    docs = build_admin_docs(fake, cfg.n_admins)
    result = ctx.col.admins.insert_many(docs)
    inserted = len(result.inserted_ids)
    tiny_logger(f"[SEED][ADMINS] Inserted {inserted} admins.")
    return inserted
