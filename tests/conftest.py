import collections
import asyncio
from dataclasses import dataclass
from typing import Any
from functools import partial

import pytest
import async_timeout
from addict import Dict
from prometheus_client import registry

from jticker_core import injector

from jticker_aggregator import candle_provider, candle_consumer, stats
from jticker_aggregator.aggregator import Aggregator


class _FakeKafka:

    def __init__(self):
        self.data = collections.defaultdict(list)
        self.subs = collections.defaultdict(set)

    def subscribe(self, topic, queue):
        self.subs[topic].add(queue)

    def put(self, topic, data):
        self.data[topic].append(data)
        offset = len(self.data[topic])
        for q in self.subs[topic]:
            q.put_nowait((topic, offset, data))


class _FakeAioKafkaConsumer:

    def __init__(self, *topics, _fake_kafka, loop, bootstrap_servers, auto_offset_reset="latest",
                 group_id=None):
        self._fake_kafka = _fake_kafka
        self.q = asyncio.Queue()
        self._offsets = collections.defaultdict(int)
        for topic in topics:
            self._fake_kafka.subscribe(topic, self.q)

    async def start(self):
        pass

    async def stop(self):
        pass

    @dataclass
    class Message:
        value: Any

    async def __aiter__(self):
        while True:
            topic, offset, value = await self.q.get()
            self._offsets[topic] = offset
            yield self.Message(value)

    def subscribe(self, topics):
        for subs in self._fake_kafka.subs.values():
            subs.discard(self.q)
        for topic in topics:
            self._fake_kafka.subscribe(topic, self.q)


@pytest.fixture(autouse=True)
def mocked_kafka(monkeypatch):
    fake_kafka = _FakeKafka()
    with monkeypatch.context() as m:
        m.setattr(candle_provider, "AIOKafkaConsumer",
                  partial(_FakeAioKafkaConsumer, _fake_kafka=fake_kafka))
        yield fake_kafka


class _FakeInflux:

    def __init__(self):
        self._measurements = collections.defaultdict(list)

    def put(self, measurement: str, value: dict):
        self._measurements[measurement].append(value)

    def get(self, measurement: str):
        return self._measurements[measurement]


class _FakeInfluxClient:

    def __init__(self, host, port, db, unix_socket, ssl, username, password, _fake_influx):
        assert host
        assert isinstance(host, str)
        assert isinstance(port, int)
        assert isinstance(db, str)
        assert isinstance(unix_socket, (type(None), str))
        assert isinstance(ssl, bool)
        assert isinstance(username, (type(None), str))
        assert isinstance(password, (type(None), str))
        self._fake_influx = _fake_influx
        self._closed = False

    async def close(self):
        self._closed = True

    async def query(self, q: str):
        assert q == "SELECT * FROM FAKE_MAPPING_TABLE;"
        data = self._fake_influx.get("FAKE_MAPPING_TABLE")
        if not data:
            return dict(results=[{}])
        columns = list(data[0]["fields"].keys())
        values = [[d["fields"][k] for k in columns] for d in data]
        return dict(
            results=[
                dict(
                    series=[
                        dict(
                            columns=columns,
                            values=values,
                        ),
                    ],
                ),
            ],
        )

    async def write(self, measurement):
        if not isinstance(measurement, list):
            measurement = [measurement]
        for m in measurement:
            self._fake_influx.put(m["measurement"], m)


@pytest.fixture(autouse=True)
def mocked_influx(monkeypatch):
    fake_influx = _FakeInflux()
    with monkeypatch.context() as m:
        m.setattr(candle_consumer, "InfluxDBClient",
                  partial(_FakeInfluxClient, _fake_influx=fake_influx))
        yield fake_influx


@pytest.fixture(autouse=True)
def config():
    d = Dict(
        kafka_trading_pairs_topic="test_kafka_trading_pairs_topic",
        kafka_candles_stuck_timeout="1",
        trading_pair_queue_timeout="0.01",
        stats_log_interval=0.1,
        influx_chunk_size="1",
        influx_host="test-influxdb",
        influx_port="123",
        influx_db="test_db",
        influx_unix_socket=None,
        influx_username=None,
        influx_password=None,
        influx_measurements_mapping="FAKE_MAPPING_TABLE",
    )
    injector.register(name="config")(lambda: d)
    injector.register(name="version")(lambda: "tests")
    return d


@pytest.fixture
async def _aggregator_stats():
    return stats.AggregatorStats()


@pytest.fixture
async def _candle_consumer(_aggregator_stats):
    return candle_consumer.CandleConsumer(aggregator_stats=_aggregator_stats)


@pytest.fixture
async def _candle_provider():
    return candle_provider.CandleProvider()


@pytest.fixture
async def not_started_aggregator(_candle_provider, _candle_consumer):
    return Aggregator(candle_provider=_candle_provider, candle_consumer=_candle_consumer)


@pytest.fixture
async def aggregator(not_started_aggregator):
    async with not_started_aggregator:
        yield not_started_aggregator


@pytest.fixture
def condition():
    async def f(condition, *, timeout=5, interval=0.01):
        async with async_timeout.timeout(timeout):
            while not condition():
                await asyncio.sleep(interval)
    return f


@pytest.fixture(autouse=True)
def cleanup_prometheus_registry():
    yield
    registry.REGISTRY._collector_to_names.clear()
    registry.REGISTRY._names_to_collectors.clear()
