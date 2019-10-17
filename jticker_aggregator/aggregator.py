import backoff
from mode import Service
from loguru import logger
from kafka.errors import UnknownMemberIdError

from jticker_core import inject, register, Rate

from .candle_provider import CandleProvider
from .candle_consumer import CandleConsumer


@register(name="aggregator")
class Aggregator(Service):

    @inject
    def __init__(self, config, candle_provider: CandleProvider, candle_consumer: CandleConsumer):
        super().__init__()
        self.log_period = float(config.stats_log_interval)
        self.candle_provider = candle_provider
        self.candle_consumer = candle_consumer

    def on_init_dependencies(self):
        return [
            self.candle_provider,
            self.candle_consumer,
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
