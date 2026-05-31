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
    return value.lower() in ("1", "true", "yes")
