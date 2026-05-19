from datetime import datetime, timezone
from typing import Any


def timestamp_to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.fromtimestamp(float(value), tz=timezone.utc)


def datetime_to_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.timestamp()
    return float(value)


def encode_timestamp_fields(payload: dict, fields: set[str]) -> dict:
    return {
        key: timestamp_to_datetime(value) if key in fields else value
        for key, value in payload.items()
    }


def decode_timestamp_fields(payload: dict, fields: set[str]) -> dict:
    return {
        key: datetime_to_timestamp(value) if key in fields else value
        for key, value in payload.items()
    }
