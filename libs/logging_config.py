import logging
import os
import sys
from typing import Any


DEFAULT_LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s %(levelname)s %(service)s %(name)s %(message)s"
VALID_LOG_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


class ServiceNameFilter(logging.Filter):
    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "service"):
            record.service = self.service_name
        return True


def parse_log_level(value: str | None = None) -> int:
    raw_value = value if value is not None else os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL)
    normalized = (raw_value or DEFAULT_LOG_LEVEL).strip().upper()
    try:
        return VALID_LOG_LEVELS[normalized]
    except KeyError as exc:
        valid_values = ", ".join(sorted(VALID_LOG_LEVELS))
        raise ValueError(f"LOG_LEVEL must be one of: {valid_values}.") from exc


def configure_logging(service_name: str, level: str | None = None) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(ServiceNameFilter(service_name))
    handler.setFormatter(logging.Formatter(LOG_FORMAT))

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(parse_log_level(level))


def format_log_fields(**fields: Any) -> str:
    return " ".join(
        f"{key}={_format_log_value(value)}"
        for key, value in fields.items()
        if value is not None
    )


def _format_log_value(value: Any) -> str:
    text = str(value)
    if text == "" or any(char.isspace() for char in text):
        return repr(text)
    return text
