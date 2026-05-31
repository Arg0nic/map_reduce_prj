import pytest

from libs.env import bool_env, float_env, int_env, required_env


def test_required_env_returns_configured_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REQUIRED_SETTING", "configured")

    assert required_env("REQUIRED_SETTING") == "configured"


@pytest.mark.parametrize("value", [None, "", "   "])
def test_required_env_rejects_missing_or_blank_values(
    monkeypatch: pytest.MonkeyPatch,
    value: str | None,
) -> None:
    if value is None:
        monkeypatch.delenv("REQUIRED_SETTING", raising=False)
    else:
        monkeypatch.setenv("REQUIRED_SETTING", value)

    with pytest.raises(RuntimeError, match="REQUIRED_SETTING environment variable is required"):
        required_env("REQUIRED_SETTING")


def test_numeric_env_helpers_use_defaults_for_missing_or_blank_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("INT_SETTING", raising=False)
    monkeypatch.setenv("FLOAT_SETTING", "")

    assert int_env("INT_SETTING", 10) == 10
    assert float_env("FLOAT_SETTING", 1.5) == 1.5


def test_numeric_env_helpers_parse_configured_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INT_SETTING", "20")
    monkeypatch.setenv("FLOAT_SETTING", "2.5")

    assert int_env("INT_SETTING", 10) == 20
    assert float_env("FLOAT_SETTING", 1.5) == 2.5


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1", True),
        ("true", True),
        ("yes", True),
        ("false", False),
        ("0", False),
        ("no", False),
    ],
)
def test_bool_env_parses_common_values(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    expected: bool,
) -> None:
    monkeypatch.setenv("BOOL_SETTING", value)

    assert bool_env("BOOL_SETTING") is expected


@pytest.mark.parametrize("value", ["tru", "maybe", "2"])
def test_bool_env_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv("BOOL_SETTING", value)

    with pytest.raises(ValueError, match="BOOL_SETTING must be a boolean value"):
        bool_env("BOOL_SETTING")
