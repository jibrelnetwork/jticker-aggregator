import collections
import asyncio
from dataclasses import dataclass
from typing import Any
from functools import partial

import pytest
from aiohttp import web
from addict import Dict
from prometheus_client import registry

from jticker_core import injector, WebServer, Interval

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
        self._started = False
        for topic in topics:
            self._fake_kafka.subscribe(topic, self.q)

    async def start(self):
        assert self._started is False
        self._started = True

    async def stop(self):
        assert self._started is True
        self._started = False

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


@pytest.fixture(autouse=True, scope="session")
def config():
    config = Dict(
        kafka_trading_pairs_topic="test_kafka_trading_pairs_topic",
        kafka_candles_stuck_timeout="1",
        trading_pair_queue_timeout="0.01",
        stats_log_interval=0.1,
        time_series_host="localhost",
        time_series_port="8086",
        time_series_allow_migrations=True,
        time_series_default_row_limit="1000",
        time_series_chunk_size="1",
    )
    injector.register(name="config")(lambda: config)
    injector.register(name="version")(lambda: "tests")
    return config


@pytest.fixture
async def _web_app():
    return web.Application()


@pytest.fixture
async def _candle_provider():
    return candle_provider.CandleProvider()


@pytest.fixture
async def _aggregator_stats():
    return stats.AggregatorStats()


@pytest.fixture
async def _candle_consumer(_aggregator_stats, time_series):
    return candle_consumer.CandleConsumer(aggregator_stats=_aggregator_stats,
                                          time_series=time_series)


@pytest.fixture
async def _web_server(_web_app):
    return WebServer(_web_app)


@pytest.fixture
async def not_started_aggregator(config, mocked_kafka, clean_influx, _web_app, _candle_provider,
                                 _candle_consumer, _web_server):
    return Aggregator(
        config=config,
        candle_provider=_candle_provider,
        candle_consumer=_candle_consumer,
        web_server=_web_server,
        web_app=_web_app,
    )


@pytest.fixture
async def aggregator(not_started_aggregator):
    async with not_started_aggregator as a:
        await a.candle_consumer._time_series.migrate()
        yield a


@pytest.fixture(autouse=True)
def cleanup_prometheus_registry():
    yield
    registry.REGISTRY._collector_to_names.clear()
    registry.REGISTRY._names_to_collectors.clear()


@pytest.fixture
def wait_candles():
    async def wait_candles_implementation(ts, tp):
        for _ in range(10):
            cs = await ts.get_candles(
                tp,
                group_interval=Interval.MIN_1,
            )
            if len(cs):
                return cs
            await asyncio.sleep(0.5)
        raise TimeoutError("No candles")
    return wait_candles_implementation
