from datetime import datetime, timezone

from libs.db_time import (
    datetime_to_timestamp,
    decode_timestamp_fields,
    encode_timestamp_fields,
    timestamp_to_datetime,
)


def test_timestamp_to_datetime_converts_epoch_to_utc_datetime() -> None:
    assert timestamp_to_datetime(0) == datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_datetime_to_timestamp_converts_aware_datetime_to_epoch_float() -> None:
    value = datetime(1970, 1, 1, 0, 1, tzinfo=timezone.utc)

    assert datetime_to_timestamp(value) == 60.0


def test_encode_timestamp_fields_converts_selected_fields() -> None:
    payload = {
        "job_id": "job-1",
        "submitted_at": 60.0,
        "status": "uploaded",
    }

    result = encode_timestamp_fields(payload, {"submitted_at"})

    assert result == {
        "job_id": "job-1",
        "submitted_at": datetime(1970, 1, 1, 0, 1, tzinfo=timezone.utc),
        "status": "uploaded",
    }


def test_decode_timestamp_fields_converts_selected_fields() -> None:
    payload = {
        "job_id": "job-1",
        "submitted_at": datetime(1970, 1, 1, 0, 1, tzinfo=timezone.utc),
        "status": "uploaded",
    }

    result = decode_timestamp_fields(payload, {"submitted_at"})

    assert result == {
        "job_id": "job-1",
        "submitted_at": 60.0,
        "status": "uploaded",
    }
