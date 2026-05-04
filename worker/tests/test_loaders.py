import json
from pathlib import Path

import pytest

from worker.loaders import jsonDataSink, jsonDataSource, txtDataSink, txtDataSource


def test_txt_data_source_reads_stripped_lines(tmp_path: Path) -> None:
    input_file = tmp_path / "input.txt"
    input_file.write_text(" alpha \n\nbeta\n", encoding="utf-8")

    assert list(txtDataSource.load(str(input_file))) == ["alpha", "", "beta"]


def test_json_data_source_reads_json_lines(tmp_path: Path) -> None:
    input_file = tmp_path / "data.jsonl"
    input_file.write_text('{"alpha": 1}\n{"beta": 2}\n', encoding="utf-8")

    assert list(jsonDataSource.load(str(input_file))) == [
        {"alpha": 1},
        {"beta": 2},
    ]


def test_json_data_source_rejects_invalid_json(tmp_path: Path) -> None:
    input_file = tmp_path / "broken.jsonl"
    input_file.write_text('{"alpha": 1}\nnot json\n', encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        list(jsonDataSource.load(str(input_file)))


def test_json_data_sink_writes_single_json_object(tmp_path: Path) -> None:
    sink = jsonDataSink(str(tmp_path), mode="json")

    sink.save({"alpha": 1, "beta": 2}, "result")

    assert json.loads((tmp_path / "result.json").read_text()) == {
        "alpha": 1,
        "beta": 2,
    }


def test_json_data_sink_writes_json_lines(tmp_path: Path) -> None:
    sink = jsonDataSink(str(tmp_path), mode="jsonl")

    sink.save({"alpha": 1, "beta": 2}, "part_0")

    lines = (tmp_path / "part_0.jsonl").read_text().splitlines()
    assert [json.loads(line) for line in lines] == [
        {"alpha": 1},
        {"beta": 2},
    ]


def test_json_data_sink_rejects_unknown_mode(tmp_path: Path) -> None:
    sink = jsonDataSink(str(tmp_path), mode="xml")

    with pytest.raises(ValueError, match="Unsupported mode"):
        sink.save({"alpha": 1}, "result")


def test_txt_data_sink_writes_key_value_lines(tmp_path: Path) -> None:
    sink = txtDataSink(str(tmp_path))

    sink.save({"alpha": 1, "beta": 2}, "result")

    assert (tmp_path / "result.txt").read_text(encoding="utf-8").splitlines() == [
        "alpha 1",
        "beta 2",
    ]
