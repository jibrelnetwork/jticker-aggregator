from mode import Service
from loguru import logger

from .injector import inject, register
from .candle_provider import CandleProvider


@register(name="aggregator")
class Aggregator(Service):

    @inject
    def __init__(self, candle_provider: CandleProvider):
        super().__init__()
        self.candle_provider = candle_provider

    def on_init_dependencies(self):
        return [
            self.candle_provider,
        ]

    async def on_started(self):
        self.add_future(self.aggregate())

    async def aggregate(self):
        logger.info("Aggregation started")
        async for candle in self.candle_provider:
            print(candle)
