#!/usr/bin/env python3
from datetime import datetime, timezone
from enum import Enum
from typing import List

def tiny_logger(msg: str) -> None:
    """
    Print a timestamped log message in UTC.

    Format example:
        2025-11-26 15:23:11.492 | message

    :param msg: The message to output.
    :return: None
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts} | {msg}", flush=True)
