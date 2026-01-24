from pymongo.database import Database


def ensure_indexes(db: Database, logs_coll: str = "logs") -> None:
    """
    Create all required MongoDB indexes for the project.

    :param db: MongoDB database handle.
    :param logs_coll: Name of the logs collection.
    :return: None
    """
    logs = db[logs_coll]
    admins = db["admins"]
    upvotes = db["upvotes"]

    logs.create_index([("logSet", 1), ("ingestKey", 1)], unique=True, name="uniq_logset_ingestkey")

    logs.create_index([("ts", 1), ("logSet", 1)], name="ts_logset")
    logs.create_index([("day", 1), ("logSet", 1)], name="day_logset")
    logs.create_index([("day", 1), ("upvoteCount", -1)], name="day_upvotes_desc")
    logs.create_index([("sourceIp", 1), ("day", 1)], name="sourceip_day")
    logs.create_index([("actionType", 1), ("ts", 1)], name="action_ts")
    logs.create_index([("blockId", 1), ("day", 1), ("actionType", 1)], name="block_day_action")

    admins.create_index([("username", 1)], unique=True, name="uniq_username")
    admins.create_index([("email", 1)], unique=True, name="uniq_email")

    upvotes.create_index([("adminId", 1), ("logId", 1)], unique=True, name="uniq_admin_log_vote")

    upvotes.create_index([("day", 1), ("adminId", 1)], name="admin_day_votes")
    upvotes.create_index([("usernameUsed", 1)], name="username_used")
    upvotes.create_index([("emailUsed", 1)], name="email_used")
    upvotes.create_index([("blockIds", 1)], name="blockids")
