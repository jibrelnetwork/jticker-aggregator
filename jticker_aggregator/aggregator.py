import backoff
from mode import Service
from loguru import logger
from kafka.errors import UnknownMemberIdError

from jticker_core import inject, register

from .candle_provider import CandleProvider
from .candle_consumer import CandleConsumer


@register(name="aggregator")
class Aggregator(Service):

    @inject
    def __init__(self, candle_provider: CandleProvider, candle_consumer: CandleConsumer):
        super().__init__()
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
        async for candle in self.candle_provider:
            await self.candle_consumer.store_candle(candle)
