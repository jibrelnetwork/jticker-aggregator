import backoff
import prometheus_client
from aiohttp import web
from kafka.errors import UnknownMemberIdError
from loguru import logger
from mode import Service

from jticker_core import Rate, WebServer, inject, register

from .candle_consumer import CandleConsumer
from .candle_provider import CandleProvider


@register(name="aggregator")
class Aggregator(Service):

    @inject
    def __init__(self, config, candle_provider: CandleProvider, candle_consumer: CandleConsumer,
                 web_server: WebServer, web_app: web.Application):
        super().__init__()
        self.log_period = float(config.stats_log_interval)
        self.candle_provider = candle_provider
        self.candle_consumer = candle_consumer
        self.web_server = web_server
        self.web_app = web_app
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
            self.candle_provider,
            self.candle_consumer,
            self.web_server,
        ]

    async def on_started(self):
        self.add_future(self.aggregate())

    @backoff.on_exception(
        backoff.constant,
        (UnknownMemberIdError,),
        jitter=None,
        interval=1)
    async def aggregate(self):
        logger.info("Aggregation started")
        r = Rate(log_period=self.log_period, log_template="{:.3f} candles/s")
        async for candle in self.candle_provider:
            r.inc()
            await self.candle_consumer.store_candle(candle)
