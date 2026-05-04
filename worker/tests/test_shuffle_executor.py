import os
from collections.abc import Iterable
from pathlib import Path

import pytest

from worker.worker import ShuffleExecutor


class PathMappedSource:
    '''
        Test source that returns spill records by requested file name.
    '''

    def __init__(self, records_by_name: dict[str, list[dict[str, int]]]):
        self.records_by_name = records_by_name
        self.loaded_paths: list[str] = []

    def load(self, filepath: str) -> Iterable[dict[str, int]]:
        self.loaded_paths.append(filepath)
        return iter(self.records_by_name[os.path.basename(filepath)])


class RecordingShuffler:
    '''
        Test shuffler that records records passed by ShuffleExecutor.
    '''

    def __init__(self):
        self.added: list[tuple[str, int]] = []
        self.flushed_with = None

    def add_record(self, word: str, count: int, sink) -> None:
        self.added.append((word, count))

    def flush(self, sink) -> None:
        self.flushed_with = sink


class RecordingSink:
    pass


def create_files(dirpath: Path, names: list[str]) -> None:
    for name in names:
        (dirpath / name).write_text("", encoding="utf-8")


def test_reads_only_spill_files_and_flushes_at_end(tmp_path: Path) -> None:
    create_files(tmp_path, ["spill_0.jsonl", "spill_1.jsonl", "ignore.jsonl"])
    source = PathMappedSource({
        "spill_0.jsonl": [{"alpha": 1, "beta": 2}],
        "spill_1.jsonl": [{"alpha": 3}],
    })
    sink = RecordingSink()
    shuffler = RecordingShuffler()
    executor = ShuffleExecutor(
        shuffler=shuffler,
        sink=sink,
        source=source,
    )

    executor.process(str(tmp_path))

    assert {os.path.basename(path) for path in source.loaded_paths} == {
        "spill_0.jsonl",
        "spill_1.jsonl",
    }
    assert sorted(shuffler.added) == [
        ("alpha", 1),
        ("alpha", 3),
        ("beta", 2),
    ]
    assert shuffler.flushed_with is sink


def test_flushes_even_when_no_spill_files_exist(tmp_path: Path) -> None:
    create_files(tmp_path, ["other.jsonl"])
    source = PathMappedSource({})
    sink = RecordingSink()
    shuffler = RecordingShuffler()
    executor = ShuffleExecutor(
        shuffler=shuffler,
        sink=sink,
        source=source,
    )

    executor.process(str(tmp_path))

    assert source.loaded_paths == []
    assert shuffler.added == []
    assert shuffler.flushed_with is sink


def test_propagates_source_errors(tmp_path: Path) -> None:
    create_files(tmp_path, ["spill_0.jsonl"])
    source = PathMappedSource({})
    sink = RecordingSink()
    shuffler = RecordingShuffler()
    executor = ShuffleExecutor(
        shuffler=shuffler,
        sink=sink,
        source=source,
    )

    with pytest.raises(KeyError):
        executor.process(str(tmp_path))


def test_propagates_invalid_spill_record_errors(tmp_path: Path) -> None:
    create_files(tmp_path, ["spill_0.jsonl"])
    source = PathMappedSource({
        "spill_0.jsonl": ["not-a-dict"],  # type: ignore[list-item]
    })
    sink = RecordingSink()
    shuffler = RecordingShuffler()
    executor = ShuffleExecutor(
        shuffler=shuffler,
        sink=sink,
        source=source,
    )

    with pytest.raises(AttributeError):
        executor.process(str(tmp_path))
