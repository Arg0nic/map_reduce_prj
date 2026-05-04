import pytest

from worker.worker import WordCountShuffler


class RecordingSink:
    '''
        Test sink that records partition files saved by WordCountShuffler.
    '''

    def __init__(self):
        self.saved: list[tuple[str, dict[str, int]]] = []

    def save(self, data: dict[str, int], name: str) -> None:
        self.saved.append((name, dict(data)))


@pytest.mark.parametrize("word", ["alpha", "beta", "hello", "\u043f\u0440\u0438\u0432\u0435\u0442", "\u4f60\u597d"])
def test_stable_bucket_is_deterministic_and_in_range(word: str) -> None:
    first_bucket = WordCountShuffler._stable_bucket(word, num_parts=4)
    second_bucket = WordCountShuffler._stable_bucket(word, num_parts=4)

    assert first_bucket == second_bucket
    assert 0 <= first_bucket < 4


def test_add_record_aggregates_same_word_in_one_partition() -> None:
    sink = RecordingSink()
    shuffler = WordCountShuffler(num_parts=4, flush_threshold=10)
    bucket = WordCountShuffler._stable_bucket("alpha", num_parts=4)

    shuffler.add_record("alpha", 2, sink=None)
    shuffler.add_record("alpha", 3, sink=None)
    shuffler.flush(sink)

    assert sink.saved == [
        (f"part_{bucket}_0", {"alpha": 5}),
    ]


def test_flush_saves_only_non_empty_partitions_and_clears_buffers() -> None:
    sink = RecordingSink()
    shuffler = WordCountShuffler(num_parts=4, flush_threshold=10)

    shuffler.add_record("alpha", 1, sink=None)
    shuffler.add_record("beta", 2, sink=None)

    shuffler.flush(sink)
    shuffler.flush(sink)

    assert sink.saved
    assert all(data for _, data in sink.saved)
    assert all(not buffer for buffer in shuffler.buffers)


def test_add_record_flushes_partition_when_threshold_is_reached() -> None:
    sink = RecordingSink()
    shuffler = WordCountShuffler(num_parts=1, flush_threshold=2)

    shuffler.add_record("alpha", 1, sink=sink)
    shuffler.add_record("beta", 2, sink=sink)
    shuffler.add_record("gamma", 3, sink=sink)
    shuffler.flush(sink)

    assert sink.saved == [
        ("part_0_0", {"alpha": 1, "beta": 2}),
        ("part_0_1", {"gamma": 3}),
    ]


def test_add_record_requires_sink_when_threshold_flush_is_needed() -> None:
    shuffler = WordCountShuffler(num_parts=1, flush_threshold=1)

    with pytest.raises(RuntimeError, match="sink required for flush"):
        shuffler.add_record("alpha", 1, sink=None)


def test_flush_increments_partition_counter() -> None:
    sink = RecordingSink()
    shuffler = WordCountShuffler(num_parts=1, flush_threshold=10)

    shuffler.add_record("alpha", 1, sink=None)
    shuffler.flush(sink)
    shuffler.add_record("beta", 2, sink=None)
    shuffler.flush(sink)

    assert sink.saved == [
        ("part_0_0", {"alpha": 1}),
        ("part_0_1", {"beta": 2}),
    ]
