from datetime import datetime, timezone

def ts_apache(s: str) -> datetime:
    # 09/Jun/2005:07:11:21 -0400
    dt = datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z")
    return dt.astimezone(timezone.utc)

def ts_hdfs_compact(date: str, time: str) -> datetime:
    # YYMMDD HHMMSS in dataset, treat as UTC
    return datetime.strptime(date + time, "%y%m%d%H%M%S").replace(tzinfo=timezone.utc)

def day_str(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).date().isoformat()
