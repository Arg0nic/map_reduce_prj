import logging

import pytest

from libs.logging_config import format_log_fields, parse_log_level


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("DEBUG", logging.DEBUG),
        ("info", logging.INFO),
        (" warning ", logging.WARNING),
        ("ERROR", logging.ERROR),
        ("CRITICAL", logging.CRITICAL),
    ],
)
def test_parse_log_level_accepts_standard_names(value: str, expected: int) -> None:
    assert parse_log_level(value) == expected


def test_parse_log_level_uses_env_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    assert parse_log_level() == logging.INFO


def test_parse_log_level_rejects_unknown_values() -> None:
    with pytest.raises(ValueError, match="LOG_LEVEL must be one of"):
        parse_log_level("verbose")


def test_format_log_fields_uses_key_value_pairs() -> None:
    assert format_log_fields(job_id="job-1", part_num=2, missing=None) == "job_id=job-1 part_num=2"


def test_format_log_fields_quotes_values_with_spaces() -> None:
    assert format_log_fields(message="hello world") == "message='hello world'"
