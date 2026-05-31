from os import getenv


def required_env(name: str) -> str:
    value = getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"{name} environment variable is required.")
    return value


def int_env(name: str, default: int) -> int:
    value = getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def float_env(name: str, default: float) -> float:
    value = getenv(name)
    if value is None or value.strip() == "":
        return default
    return float(value)


def bool_env(name: str, default: bool = False) -> bool:
    value = getenv(name)
    if value is None or value.strip() == "":
        return default

    normalized = value.strip().lower()
    if normalized in ("1", "true", "yes"):
        return True
    if normalized in ("0", "false", "no"):
        return False

    raise ValueError(f"{name} must be a boolean value: true/false, yes/no, or 1/0.")
