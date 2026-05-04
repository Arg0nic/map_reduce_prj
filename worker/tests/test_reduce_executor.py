import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from worker.worker import ReduceExecutor, Reducer


class PathMappedSource:
    '''
        Test source that returns records based on requested file name.
    '''

    def __init__(self, records_by_name: dict[str, list[dict[str, int]]]):
        self.records_by_name = records_by_name
        self.loaded_paths: list[str] = []

    def load(self, filepath: str) -> Iterable[dict[str, int]]:
        self.loaded_paths.append(filepath)
        return iter(self.records_by_name[os.path.basename(filepath)])


class RecordingSink:
    '''
        Test sink that records reduced data saved by ReduceExecutor.
    '''

    def __init__(self):
        self.saved: list[tuple[str, dict[str, int]]] = []

    def save(self, data: dict[str, int], name: str) -> None:
        self.saved.append((name, dict(data)))


class RecordingReducer(Reducer):
    '''
        Test reducer that records received data and returns a prepared result.
    '''

    def __init__(self, result: dict[str, int]):
        self.result = result
        self.received_data: list[dict[str, int]] = []

    def do_reduce(self, mapped_data: Iterable[dict[Any, int]]) -> dict[str, int]:
        self.received_data = [dict(record) for record in mapped_data]
        return self.result


class FailingReducer(Reducer):
    def do_reduce(self, mapped_data: Iterable[dict[Any, int]]) -> dict[str, int]:
        raise ValueError("reduce failed")


def create_partition_files(part_dir: Path, names: list[str]) -> None:
    for name in names:
        (part_dir / name).write_text("", encoding="utf-8")


def test_reads_all_partition_files_and_saves_reduced_result(tmp_path: Path) -> None:
    create_partition_files(tmp_path, ["part_0_0.jsonl", "part_0_1.jsonl"])
    source = PathMappedSource({
        "part_0_0.jsonl": [{"alpha": 1}, {"beta": 2}],
        "part_0_1.jsonl": [{"alpha": 3}],
    })
    sink = RecordingSink()
    reducer = RecordingReducer(result={"alpha": 4, "beta": 2})
    executor = ReduceExecutor(
        worker=reducer,
        sink=sink,
        source=source,
    )

    executor.process(str(tmp_path), part_num=0)

    assert {os.path.basename(path) for path in source.loaded_paths} == {
        "part_0_0.jsonl",
        "part_0_1.jsonl",
    }
    assert len(reducer.received_data) == 3
    assert {"alpha": 1} in reducer.received_data
    assert {"beta": 2} in reducer.received_data
    assert {"alpha": 3} in reducer.received_data
    assert sink.saved == [
        ("reduced_0", {"alpha": 4, "beta": 2}),
    ]


def test_saves_reducer_result_for_empty_partition_directory(tmp_path: Path) -> None:
    source = PathMappedSource({})
    sink = RecordingSink()
    reducer = RecordingReducer(result={})
    executor = ReduceExecutor(
        worker=reducer,
        sink=sink,
        source=source,
    )

    executor.process(str(tmp_path), part_num=3)

    assert source.loaded_paths == []
    assert reducer.received_data == []
    assert sink.saved == [
        ("reduced_3", {}),
    ]


def test_uses_part_num_in_output_name(tmp_path: Path) -> None:
    create_partition_files(tmp_path, ["part_2_0.jsonl"])
    source = PathMappedSource({
        "part_2_0.jsonl": [{"gamma": 7}],
    })
    sink = RecordingSink()
    reducer = RecordingReducer(result={"gamma": 7})
    executor = ReduceExecutor(
        worker=reducer,
        sink=sink,
        source=source,
    )

    executor.process(str(tmp_path), part_num=2)

    assert sink.saved == [
        ("reduced_2", {"gamma": 7}),
    ]


def test_propagates_source_errors(tmp_path: Path) -> None:
    create_partition_files(tmp_path, ["unknown.jsonl"])
    source = PathMappedSource({})
    sink = RecordingSink()
    reducer = RecordingReducer(result={})
    executor = ReduceExecutor(
        worker=reducer,
        sink=sink,
        source=source,
    )

    with pytest.raises(KeyError):
        executor.process(str(tmp_path), part_num=0)

    assert sink.saved == []


def test_propagates_reducer_errors(tmp_path: Path) -> None:
    create_partition_files(tmp_path, ["part_0_0.jsonl"])
    source = PathMappedSource({
        "part_0_0.jsonl": [{"alpha": 1}],
    })
    sink = RecordingSink()
    executor = ReduceExecutor(
        worker=FailingReducer(),
        sink=sink,
        source=source,
    )

    with pytest.raises(ValueError, match="reduce failed"):
        executor.process(str(tmp_path), part_num=0)

    assert sink.saved == []
