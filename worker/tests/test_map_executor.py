from collections.abc import Iterable
from typing import Any

import pytest

from worker.worker import MapExecutor, Mapper


MappedPair = tuple[str, int]


class InMemorySource:
    '''
        Test source that returns prepared records instead of reading a real file.
    '''

    def __init__(self, records: list[object]):
        self.records = records
        self.loaded_paths: list[str] = []

    def load(self, filepath: str) -> Iterable[object]:
        self.loaded_paths.append(filepath)
        return iter(self.records)


class RecordingSink:
    '''
        Test sink that records every spill saved by MapExecutor.
    '''

    def __init__(self):
        self.saved: list[tuple[str, dict[str, int]]] = []

    def save(self, data: dict[str, int], name: str) -> None:
        self.saved.append((name, dict(data)))


class PairMapper(Mapper):
    '''
        Test mapper for MapExecutor tests.

        It receives ready key-count pairs, so tests can focus on executor
        behavior instead of word-count tokenization.
    '''

    @staticmethod
    def do_map(data: Any) -> Iterable[MappedPair]:
        return data


class FailingMapper(Mapper):
    @staticmethod
    def do_map(data: Any) -> Iterable[MappedPair]:
        raise ValueError("mapping failed")


class BadCountMapper(Mapper):
    @staticmethod
    def do_map(data: Any) -> Iterable[tuple[str, Any]]:
        return [("value", "not-an-int")]


def test_reads_source_and_saves_aggregated_mapper_output() -> None:
    source = InMemorySource([
        [("alpha", 1), ("beta", 2)],
        [("alpha", 3)],
    ])
    sink = RecordingSink()
    executor = MapExecutor(
        worker=PairMapper(),
        sink=sink,
        source=source,
    )

    executor.process("input.data")

    assert source.loaded_paths == ["input.data"]
    assert sink.saved == [
        ("spill_0", {"alpha": 4, "beta": 2}),
    ]


def test_does_not_save_spill_when_mapper_emits_no_records() -> None:
    source = InMemorySource([[], []])
    sink = RecordingSink()
    executor = MapExecutor(
        worker=PairMapper(),
        sink=sink,
        source=source,
    )

    executor.process("empty.data")

    assert sink.saved == []


def test_flushes_when_unique_key_threshold_is_reached() -> None:
    source = InMemorySource([
        [("alpha", 1), ("beta", 1), ("alpha", 4), ("gamma", 1)],
    ])
    sink = RecordingSink()
    executor = MapExecutor(
        worker=PairMapper(),
        sink=sink,
        source=source,
        threshold=2,
    )

    executor.process("input.data")

    assert sink.saved == [
        ("spill_0", {"alpha": 1, "beta": 1}),
        ("spill_1", {"alpha": 4, "gamma": 1}),
    ]


def test_saves_remaining_buffer_after_threshold_flush() -> None:
    source = InMemorySource([
        [("alpha", 1), ("beta", 1), ("gamma", 1)],
    ])
    sink = RecordingSink()
    executor = MapExecutor(
        worker=PairMapper(),
        sink=sink,
        source=source,
        threshold=2,
    )

    executor.process("input.data")

    assert sink.saved == [
        ("spill_0", {"alpha": 1, "beta": 1}),
        ("spill_1", {"gamma": 1}),
    ]


def test_propagates_mapper_errors() -> None:
    source = InMemorySource(["broken-record"])
    sink = RecordingSink()
    executor = MapExecutor(
        worker=FailingMapper(),
        sink=sink,
        source=source,
    )

    with pytest.raises(ValueError, match="mapping failed"):
        executor.process("broken.data")


def test_rejects_mapper_output_with_non_numeric_count() -> None:
    source = InMemorySource(["bad-count"])
    sink = RecordingSink()
    executor = MapExecutor(
        worker=BadCountMapper(),
        sink=sink,
        source=source,
    )

    with pytest.raises(TypeError):
        executor.process("bad-count.data")
