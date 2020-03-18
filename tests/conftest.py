import pytest
from addict import Dict
from aiohttp import web
from prometheus_client import registry

from jticker_aggregator import stats
from jticker_aggregator.aggregator import Aggregator
from jticker_core import WebServer, injector


@pytest.fixture(autouse=True, scope="session")
def config():
    config = Dict(
        stats_log_interval=0.1,
        time_series_host="localhost",
        time_series_port="8086",
        time_series_allow_migrations=True,
        time_series_default_row_limit="1000",
        time_series_chunk_size="1",
        time_series_client_timeout="10",
        stream_storage_type="postgres",
        stream_storage_postgres_host="localhost",
        stream_storage_postgres_port="5432",
        stream_storage_postgres_poller_name="test",
        stream_storage_postgres_read_batch_size="1",
        stream_storage_postgres_write_batch_size="1",
        stream_storage_postgres_wait_data_timeout="0",
    )
    injector.register(name="config")(lambda: config)
    injector.register(name="version")(lambda: "tests")
    return config


@pytest.fixture
async def _web_app():
    return web.Application()


@pytest.fixture
async def _aggregator_stats():
    return stats.AggregatorStats()


@pytest.fixture
async def _web_server(_web_app):
    return WebServer(_web_app)


@pytest.fixture
async def not_started_aggregator(config, clean_influx, clean_postgres, _web_server, _web_app,
                                 _aggregator_stats):
    return Aggregator(web_server=_web_server, web_app=_web_app, aggregator_stats=_aggregator_stats)


@pytest.fixture
async def aggregator(not_started_aggregator):
    async with not_started_aggregator as a:
        yield a


@pytest.fixture(autouse=True)
def cleanup_prometheus_registry():
    yield
    registry.REGISTRY._collector_to_names.clear()
    registry.REGISTRY._names_to_collectors.clear()
