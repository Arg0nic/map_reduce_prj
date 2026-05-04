import pytest

from worker.worker import WordCountReducer


def test_sums_counts_for_same_words() -> None:
    mapped_data = [
        {"alpha": 1, "beta": 2},
        {"alpha": 3, "gamma": 4},
        {"beta": 5},
    ]

    assert WordCountReducer.do_reduce(mapped_data) == {
        "alpha": 4,
        "beta": 7,
        "gamma": 4,
    }


def test_returns_empty_dict_for_empty_input() -> None:
    assert WordCountReducer.do_reduce([]) == {}


def test_handles_zero_and_negative_counts() -> None:
    mapped_data = [
        {"alpha": 5},
        {"alpha": -2},
        {"beta": 0},
    ]

    assert WordCountReducer.do_reduce(mapped_data) == {
        "alpha": 3,
        "beta": 0,
    }


def test_handles_unicode_keys() -> None:
    mapped_data = [
        {"\u043f\u0440\u0438\u0432\u0435\u0442": 2},
        {"\u043f\u0440\u0438\u0432\u0435\u0442": 3},
        {"\u4f60\u597d": 4},
    ]

    assert WordCountReducer.do_reduce(mapped_data) == {
        "\u043f\u0440\u0438\u0432\u0435\u0442": 5,
        "\u4f60\u597d": 4,
    }


def test_does_not_modify_input_records() -> None:
    first = {"alpha": 1}
    second = {"alpha": 2, "beta": 3}
    mapped_data = [first, second]

    WordCountReducer.do_reduce(mapped_data)

    assert first == {"alpha": 1}
    assert second == {"alpha": 2, "beta": 3}


def test_rejects_non_dict_record() -> None:
    with pytest.raises(AttributeError):
        WordCountReducer.do_reduce([["alpha", 1]])  # type: ignore[list-item]


def test_rejects_non_numeric_count() -> None:
    with pytest.raises(TypeError):
        WordCountReducer.do_reduce([{"alpha": "bad"}])  # type: ignore[dict-item]
