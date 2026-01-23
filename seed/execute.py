from common import (
    connect,
    counts,
    ensure_core_indexes,
    fail_if_exists,
    load_seed_config,
    require_non_empty,
    seed_random,
)
from admins import load_admins_config, seed_admins
from upvotes import load_upvotes_config, seed_upvotes
from tiny_logger import tiny_logger

"""
Unified seeding entrypoint.

This script loads shared MongoDB configuration once, establishes a single DB
connection, ensures shared indexes, then executes the seed steps in order.
"""


def main() -> None:
    """
    Execute the full seeding pipeline.

    Order:
        1. Seed admins.
        2. Seed upvotes.

    :param None: This function does not accept any parameters.
    :return: None
    """
    tiny_logger("[SEED] START")

    cfg = load_seed_config()
    seed_random(cfg)
    ctx = connect(cfg)

    ensure_core_indexes(ctx)

    L, A, U = counts(ctx)
    require_non_empty("logs", L, "No logs found. Run ingestion first.")
    fail_if_exists(cfg.mode, U, "Upvotes already exist. Set MODE=topup or clear the upvotes collection.")

    seed_admins(ctx, load_admins_config())

    A2 = ctx.col.admins.count_documents({})
    require_non_empty("admins", A2, "No admins found. Run admins seed first.")

    inserted_votes = seed_upvotes(ctx, load_upvotes_config())

    tiny_logger(f"[SEED] Inserted votes: {inserted_votes}")
    tiny_logger("[SEED] END")


if __name__ == "__main__":
    """
    Script entry point.
    """
    main()
