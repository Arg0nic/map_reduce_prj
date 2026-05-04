import pytest

from worker.worker import WordCountMapper


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (
            "Hello, world! HELLO",
            [("hello", 1), ("world", 1), ("hello", 1)],
        ),
        (
            "  hello...   map-reduce\n",
            [("hello", 1), ("map", 1), ("reduce", 1)],
        ),
        (
            "Version 2 test_1",
            [("version", 1), ("2", 1), ("test_1", 1)],
        ),
    ],
)
def test_emits_words_from_common_text(line: str, expected: list[tuple[str, int]]) -> None:
    assert list(WordCountMapper.do_map(line)) == expected


@pytest.mark.parametrize("line", ["", "   ", "\n\t", ".,!?;:()[]{}"])
def test_ignores_empty_or_punctuation_only_lines(line: str) -> None:
    assert list(WordCountMapper.do_map(line)) == []


def test_handles_unicode_words_and_hieroglyphs() -> None:
    line = "\u041f\u0440\u0438\u0432\u0435\u0442 \u043c\u0438\u0440 \u4f60\u597d\uff0c\u4e16\u754c \u6771\u4eac"

    assert list(WordCountMapper.do_map(line)) == [
        ("\u043f\u0440\u0438\u0432\u0435\u0442", 1),
        ("\u043c\u0438\u0440", 1),
        ("\u4f60\u597d", 1),
        ("\u4e16\u754c", 1),
        ("\u6771\u4eac", 1),
    ]


def test_normalizes_unicode_text() -> None:
    line = "\uff28\uff45\uff4c\uff4c\uff4f \uff11\uff12\uff13 \ufb01sh"

    assert list(WordCountMapper.do_map(line)) == [
        ("hello", 1),
        ("123", 1),
        ("fish", 1),
    ]


@pytest.mark.parametrize("bad_line", [None, 123, {"text": "hello"}])
def test_rejects_non_string_input(bad_line: object) -> None:
    with pytest.raises(TypeError):
        list(WordCountMapper.do_map(bad_line))  # type: ignore[arg-type]
