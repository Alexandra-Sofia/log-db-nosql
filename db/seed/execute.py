# execute.py
from admins import load_admins_config, seed_admins
from common import (
    connect,
    counts,
    ensure_core_indexes,
    fail_if_exists,
    load_seed_config,
    require_non_empty,
    seed_random,
)
from tiny_logger import tiny_logger
from upvotes import load_upvotes_config, seed_upvotes

"""
Unified seeding entrypoint.

This script loads MongoDB configuration once, establishes a single DB connection,
ensures shared indexes, then executes seeding steps in order:
    1) admins
    2) upvotes
"""


def main() -> None:
    """
    Execute the full seeding pipeline.

    :param None: This function does not accept any parameters.
    :return: None
    """
    tiny_logger("[SEED] START")

    cfg = load_seed_config()
    tiny_logger(f"[SEED] MODE={cfg.mode} SEED={cfg.seed} DB={cfg.mongo.mongo_db}")

    seed_random(cfg)
    ctx = connect(cfg)

    ensure_core_indexes(ctx)

    L, A, U = counts(ctx)
    require_non_empty("logs", L, "No logs found. Run ingestion first.")
    fail_if_exists(cfg.mode, U, "Upvotes already exist. Set MODE=topup or clear the upvotes collection.")

    inserted_admins = seed_admins(ctx, load_admins_config())

    L2, A2, U2 = counts(ctx)
    tiny_logger(f"[SEED] After admins: logs={L2} admins={A2} upvotes={U2} inserted_admins={inserted_admins}")
    require_non_empty("admins", A2, "No admins found. Run admins seed first.")

    inserted_votes = seed_upvotes(ctx, load_upvotes_config())

    L3, A3, U3 = counts(ctx)
    tiny_logger(f"[SEED] After upvotes: logs={L3} admins={A3} upvotes={U3} inserted_votes={inserted_votes}")

    tiny_logger("[SEED] END")


if __name__ == "__main__":
    """
    Script entry point.

    :param None: No parameters are accepted.
    :return: None
    """
    main()
