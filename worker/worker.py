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
    @staticmethod
    @abstractmethod
    def do_map(data: Any) -> Iterable[tuple[Any, Any]]:
        pass


class Reducer(ABC):
    @staticmethod
    @abstractmethod
    def do_reduce(mapped_data: Iterable[dict[Any, int]]) -> dict[Any, int]:
        pass


class HealthCheck(ABC):
    '''
     Interface for health check
    '''

    @abstractmethod
    def is_healthy(self) -> bool:
        pass


class WordCountMapper(Mapper):
    '''
        Mapper for word count
    '''

    token_pattern = re.compile(r"\w+", re.UNICODE)

    @staticmethod
    def do_map(line: str) -> Iterable[MappedItem]:
        line = unicodedata.normalize('NFKC', line)
        for w in WordCountMapper.token_pattern.findall(line):
            yield (w.lower(), 1)


class WordCountReducer(Reducer):
    '''
        Reducer for word count
    '''

    @staticmethod
    def do_reduce(mapped_data: Iterable[WordCountRecord]) -> WordCountRecord:
        reduced_data: WordCountRecord = {}
        for partial_result in mapped_data:
            for word, count in partial_result.items():
                reduced_data[word] = reduced_data.get(word, 0) + count
        return reduced_data


class MapExecutor:
    '''
        Executor for the mapping phase
    '''

    def __init__(
        self,
        worker: Mapper,
        sink: DataSink,
        source: DataSource,
        threshold: int = 50_000,
    ):
        self.worker = worker
        self.sink = sink
        self.source = source
        self.threshold = threshold
        self.buffer: WordCountRecord = {}
        self.counter: int = 0

    def process(self, filepath: str) -> None:
        datasource = self.source.load(filepath)
        for line in datasource:
            for word, count in self.worker.do_map(line):
                self.buffer[word] = self.buffer.get(word, 0) + count

                if len(self.buffer) >= self.threshold:
                    self.sink.save(self.buffer, f"spill_{self.counter}")
                    self.counter += 1
                    self.buffer = {}

        if self.buffer:
            self.sink.save(self.buffer, f"spill_{self.counter}")
            self.counter += 1


class WordCountShuffler:
    def __init__(self, num_parts: int = 4, flush_threshold: int = 50_000):
        self.num_parts = num_parts
        self.flush_threshold = flush_threshold
        self.buffers: list[defaultdict[str, int]] = [defaultdict(int) for _ in range(num_parts)]
        self.part_counters: list[int] = [0] * num_parts

    def add_record(self, word: str, count: int, sink: DataSink | None) -> None:
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
        for i, data in enumerate(self.buffers):
            if data:
                sink.save(dict(data), name=f"part_{i}_{self.part_counters[i]}")
                self.part_counters[i] += 1
                data.clear()

    @staticmethod
    def _stable_bucket(key: str, num_parts: int) -> int:
        ''' Returns a stable bucket index for the given key.
         by hashing the key using zlib.crc32.'''
        return (zlib.crc32(key.encode('utf-8')) & 0xffffffff) % num_parts


class ShuffleExecutor:
    def __init__(self, shuffler: WordCountShuffler, sink: DataSink, source: DataSource):
        self.shuffler = shuffler
        self.sink = sink
        self.source = source

    def process(self, spill_dir: str) -> None:
        for file in os.listdir(spill_dir):
            if file.startswith("spill_"):
                filepath = os.path.join(spill_dir, file)
                for record in self.source.load(filepath):
                    for word, count in record.items():
                        self.shuffler.add_record(word, count, self.sink)

        self.shuffler.flush(self.sink)


class ReduceExecutor:
    '''
        Executor for the reducing phase
    '''

    def __init__(self, worker: Reducer, sink: DataSink, source: DataSource):
        self.worker = worker
        self.sink = sink
        self.source = source

    def process(self, part_dir: str, part_num: int) -> None:
        mapped_data: list[WordCountRecord] = []
        for file in os.listdir(part_dir):
            filepath = os.path.join(part_dir, file)
            for record in self.source.load(filepath):
                mapped_data.append(record)

        reduced_data = self.worker.do_reduce(mapped_data)
        self.sink.save(reduced_data, name=f'reduced_{part_num}')


class DataManager:
    @classmethod
    def manage_reduce_data(cls, source: DataSource, sink: DataSink, dirpath: str) -> WordCountRecord:
        '''
            Manages reduced data by combining all reduced parts into a single dictionary.
        '''
        combined_data: WordCountRecord = {}
        for file in os.listdir(dirpath):
            filepath = os.path.join(dirpath, file)
            for record in source.load(filepath):
                combined_data.update(record)

        combined_data = dict(sorted(combined_data.items(), key=lambda item: item[0]))
        sink.save(combined_data, name='result')
        return combined_data
