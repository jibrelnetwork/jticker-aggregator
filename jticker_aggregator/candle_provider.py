import asyncio

import backoff
from mode import Service
from addict import Dict
from loguru import logger
from kafka.errors import NoBrokersAvailable

from jticker_core import TradingPair, Candle, register, inject

from .kafka_wrapper import Consumer


@register(singleton=True, name="candle_provider")
class CandleProvider(Service):

    @inject
    def __init__(self, config: Dict):
        super().__init__()
        self.config = config
        self.trading_pairs = Dict()
        self.trading_pairs_queue: asyncio.Queue = asyncio.Queue(maxsize=10 ** 3)
        self.trading_pairs_consumer = None
        self.candles_consumer = None

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, NoBrokersAvailable),
        max_time=5 * 60)
    async def on_start(self):
        self.trading_pairs_consumer = Consumer(
            self.config.kafka_trading_pairs_topic,
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            auto_offset_reset="earliest",
        )
        await self.trading_pairs_consumer.start()
        self.candles_consumer = Consumer(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            auto_offset_reset="earliest",
            group_id=self.config.kafka_candles_consumer_group_id,
        )
        await self.candles_consumer.start()
        self.add_future(self._consume_trading_pairs())
        self.add_future(self._subscribe_trading_pairs())

    async def on_stop(self):
        if self.trading_pairs_consumer is not None:
            await self.trading_pairs_consumer.stop()
        if self.candles_consumer is not None:
            await self.candles_consumer.stop()

    async def _consume_trading_pairs(self):
        async for message in self.trading_pairs_consumer:
            logger.debug("Trading pair message received: {}", message)
            try:
                tp = TradingPair.from_json(message.value)
            except Exception:
                logger.exception("Exception on trading pair parsing")
            else:
                await self.trading_pairs_queue.put(tp)

    async def _subscribe_trading_pairs(self):
        updated = False
        while True:
            try:
                tp = await asyncio.wait_for(self.trading_pairs_queue.get(), 1)
            except asyncio.TimeoutError:
                if not updated:
                    continue
                topics = list(self.trading_pairs)
                logger.info("Update candles subscribe topics (count: {})", len(topics))
                await self.candles_consumer.subscribe(topics=topics)
                updated = False
            else:
                if tp.topic in self.trading_pairs:
                    logger.debug("Update trading pair {} to {}", self.trading_pairs[tp.topic], tp)
                else:
                    logger.debug("New trading pair {}", tp)
                self.trading_pairs[tp.topic] = tp
                updated = True

    async def __aiter__(self):
        async for message in self.candles_consumer:
            logger.debug("Candle message received: {}", message)
            try:
                candle = Candle.from_json(message.value)
            except Exception:
                logger.exception("Exception on candle parsing")
            else:
                yield candle
