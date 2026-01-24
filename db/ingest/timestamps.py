from datetime import datetime, timezone

"""
Timestamp parsing utilities.

This module provides helpers for parsing timestamps from different log formats
and normalizing them to UTC. It also provides a helper for extracting the
ISO-formatted day string used for aggregation and indexing.
"""


def ts_apache(value: str) -> datetime:
    """
    Parse an Apache access log timestamp and normalize it to UTC.

    Format example:
        09/Jun/2005:07:11:21 -0400

    :param value: Apache-formatted timestamp string.
    :return: datetime
    """
    parsed = datetime.strptime(value, "%d/%b/%Y:%H:%M:%S %z")
    return parsed.astimezone(timezone.utc)


def ts_hdfs_compact(date: str, time: str) -> datetime:
    """
    Parse a compact HDFS timestamp and interpret it as UTC.

    The dataset provides timestamps in the following format:
        date: YYMMDD
        time: HHMMSS

    These values are treated as UTC without timezone conversion.

    :param date: Date component in YYMMDD format.
    :param time: Time component in HHMMSS format.
    :return: datetime
    """
    return datetime.strptime(
        date + time,
        "%y%m%d%H%M%S",
    ).replace(tzinfo=timezone.utc)


def day_str(dt: datetime) -> str:
    """
    Convert a datetime value to an ISO-formatted UTC day string.

    The returned value is used for grouping and indexing by day.

    Format example:
        2026-01-23

    :param dt: Datetime value.
    :return: str
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).date().isoformat()
