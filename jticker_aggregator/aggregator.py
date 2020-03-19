import backoff
import prometheus_client
from aiohttp import web
from loguru import logger
from mode import Service

from jticker_core import (AbstractStreamStorage, AbstractTimeSeriesStorage, Rate,
                          StreamStorageException, TimeSeriesException, WebServer, inject, register)

from .stats import AggregatorStats


@register(name="aggregator")
class Aggregator(Service):

    @inject
    def __init__(self, config, stream_storage: AbstractStreamStorage,
                 time_series: AbstractTimeSeriesStorage,
                 web_server: WebServer, web_app: web.Application,
                 aggregator_stats: AggregatorStats):
        super().__init__()
        self.stream_storage = stream_storage
        self.time_series = time_series
        self.web_server = web_server
        self.web_app = web_app
        self.stats = aggregator_stats
        self.log_period = float(config.stats_log_interval)
        self._time_series_chunk_size = int(config.time_series_chunk_size)
        self.configure_router()

    def configure_router(self):
        router = self.web_app.router
        router.add_route("GET", "/healthcheck", self.healthcheck)
        router.add_route("GET", "/metrics", self.metrics)

    @staticmethod
    async def healthcheck(request):
        return web.json_response(dict(healthy=True))

    @staticmethod
    async def metrics(request):
        body = prometheus_client.exposition.generate_latest().decode("utf-8")
        content_type = prometheus_client.exposition.CONTENT_TYPE_LATEST
        return web.Response(body=body, headers={"Content-Type": content_type})

    def on_init_dependencies(self):
        return [
            self.stream_storage,
            self.time_series,
            self.web_server,
            self.stats,
        ]

    async def on_started(self):
        await self.time_series.migrate()
        self.add_future(self.aggregate())

    @backoff.on_exception(
        backoff.constant,
        (StreamStorageException, TimeSeriesException),
        jitter=None,
        interval=1)
    async def aggregate(self):
        logger.info("aggregation started")
        r = Rate(log_period=self.log_period, log_template="{:.3f} candles/s")
        chunk = []
        async for candle in self.stream_storage.poll_candles():
            chunk.append(candle)
            r.inc()
            self.stats.candle_stored(candle)
            if len(chunk) < self._time_series_chunk_size:
                continue
            await self.time_series.add_candles(chunk)
            chunk.clear()
            await self.stream_storage.commit_candles_position()
