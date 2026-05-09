import os
import re
import unicodedata
import zlib
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from worker.loaders import DataSink, DataSource


WordCountRecord = dict[str, int]
MappedItem = tuple[str, int]


class Mapper(ABC):
    '''
        Base interface for map-stage implementations.

        A mapper receives one source item, usually one text line, and emits
        key-value pairs that will later be aggregated and shuffled.
    '''

    @staticmethod
    @abstractmethod
    def do_map(data: Any) -> Iterable[tuple[Any, Any]]:
        '''
            Converts one input item into an iterable of mapped key-value pairs.
        '''
        pass


class Reducer(ABC):
    '''
        Base interface for reduce-stage implementations.

        A reducer receives grouped/intermediate records and combines values
        that belong to the same key.
    '''

    @staticmethod
    @abstractmethod
    def do_reduce(mapped_data: Iterable[dict[Any, int]]) -> dict[Any, int]:
        '''
            Aggregates intermediate records into one reduced dictionary.
        '''
        pass
        

class WordCountMapper(Mapper):
    '''
        Mapper for word count.

        It normalizes unicode text, extracts word-like tokens, lowercases them,
        and emits one (word, 1) pair per occurrence.
    '''

    token_pattern = re.compile(r"\w+", re.UNICODE)

    @staticmethod
    def do_map(line: str) -> Iterable[MappedItem]:
        '''
            Maps one text line into lowercase word-count pairs.
        '''
        line = unicodedata.normalize('NFKC', line)
        for w in WordCountMapper.token_pattern.findall(line):
            yield (w.lower(), 1)


class WordCountReducer(Reducer):
    '''
        Reducer for word count.

        It combines partial word-count dictionaries produced by map/shuffle
        processing into one dictionary of total counts.
    '''

    @staticmethod
    def do_reduce(mapped_data: Iterable[WordCountRecord]) -> WordCountRecord:
        '''
            Sums counts for every word across all partial result dictionaries.
        '''
        reduced_data: WordCountRecord = {}
        for partial_result in mapped_data:
            for word, count in partial_result.items():
                reduced_data[word] = reduced_data.get(word, 0) + count
        return reduced_data


class MapExecutor:
    '''
        Executor for the mapping phase.

        It streams input records from a DataSource, applies a Mapper, keeps an
        in-memory aggregation buffer, and writes spill files through a DataSink.
    '''

    def __init__(
        self,
        worker: Mapper,
        sink: DataSink,
        source: DataSource,
        threshold: int = 50_000,
    ):
        '''
            Creates a map executor.

            threshold controls how many unique keys can be held before the
            current buffer is saved as a spill file.
        '''
        self.worker = worker
        self.sink = sink
        self.source = source
        self.threshold = threshold

    def process(self, filepath: str) -> None:
        '''
            Processes one input file and writes one or more spill outputs.
        '''
        buffer: WordCountRecord = {}
        counter = 0
        datasource = self.source.load(filepath)
        for line in datasource:
            for word, count in self.worker.do_map(line):
                buffer[word] = buffer.get(word, 0) + count

                if len(buffer) >= self.threshold:
                    self.sink.save(buffer, f"spill_{counter}")
                    counter += 1
                    buffer = {}

        if buffer:
            self.sink.save(buffer, f"spill_{counter}")


class WordCountShuffler:
    '''
        Groups mapped word counts into stable reduce partitions.

        The same word always goes to the same partition, which lets each reduce
        worker aggregate a complete subset of the key space.
    '''

    def __init__(self, num_parts: int = 4, flush_threshold: int = 50_000):
        '''
            Creates a shuffler with a fixed number of reduce partitions.

            flush_threshold controls how many unique keys can be buffered in a
            partition before it is written to the sink.
        '''
        self.num_parts = num_parts
        self.flush_threshold = flush_threshold
        self.buffers: list[defaultdict[str, int]] = [defaultdict(int) for _ in range(num_parts)]
        self.part_counters: list[int] = [0] * num_parts

    def add_record(self, word: str, count: int, sink: DataSink | None) -> None:
        '''
            Adds one mapped word-count pair to the correct reduce partition.

            If a partition buffer reaches flush_threshold, that partition is
            immediately saved and cleared.
        '''
        bucket = self._stable_bucket(word, self.num_parts)
        buf = self.buffers[bucket]
        buf[word] += count
        if len(buf) >= self.flush_threshold:
            if sink is None:
                raise RuntimeError("sink required for flush")
            sink.save(dict(buf), name=f"part_{bucket}_{self.part_counters[bucket]}")
            self.part_counters[bucket] += 1
            buf.clear()

    def flush(self, sink: DataSink) -> None:
        '''
            Writes all non-empty partition buffers to the sink.
        '''
        for i, data in enumerate(self.buffers):
            if data:
                sink.save(dict(data), name=f"part_{i}_{self.part_counters[i]}")
                self.part_counters[i] += 1
                data.clear()

    @staticmethod
    def _stable_bucket(key: str, num_parts: int) -> int:
        '''
            Returns a stable bucket index for the given key.

            crc32 is used instead of Python's built-in hash because built-in
            hash randomization can change bucket assignments between processes.
        '''
        return (zlib.crc32(key.encode('utf-8')) & 0xffffffff) % num_parts


class ShuffleExecutor:
    '''
        Executor for the shuffle phase.

        It reads local spill files, feeds records into WordCountShuffler, and
        writes partitioned shuffle files for the reduce phase.
    '''

    def __init__(self, shuffler: WordCountShuffler, sink: DataSink, source: DataSource):
        '''
            Creates a shuffle executor from a shuffler, sink, and source.
        '''
        self.shuffler = shuffler
        self.sink = sink
        self.source = source

    def process(self, spill_dir: str) -> None:
        '''
            Processes all spill_* files in a directory and flushes partitions.
        '''
        for file in os.listdir(spill_dir):
            if file.startswith("spill_"):
                filepath = os.path.join(spill_dir, file)
                for record in self.source.load(filepath):
                    for word, count in record.items():
                        self.shuffler.add_record(word, count, self.sink)

        self.shuffler.flush(self.sink)


class ReduceExecutor:
    '''
        Executor for the reducing phase.

        It reads all shuffle fragments for one partition, applies a Reducer,
        and writes one reduced output file for that partition.
    '''

    def __init__(self, worker: Reducer, sink: DataSink, source: DataSource):
        '''
            Creates a reduce executor from a reducer, sink, and source.
        '''
        self.worker = worker
        self.sink = sink
        self.source = source

    def process(self, part_dir: str, part_num: int) -> None:
        '''
            Reduces all records in one partition directory.
        '''
        mapped_data: list[WordCountRecord] = []
        for file in os.listdir(part_dir):
            filepath = os.path.join(part_dir, file)
            for record in self.source.load(filepath):
                mapped_data.append(record)

        reduced_data = self.worker.do_reduce(mapped_data)
        self.sink.save(reduced_data, name=f'reduced_{part_num}')
