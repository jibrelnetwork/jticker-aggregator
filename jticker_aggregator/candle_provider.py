import asyncio

import backoff
from mode import Service
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import ConnectionError
from addict import Dict
from loguru import logger

from jticker_core import TradingPair, Candle, register, inject, normalize_kafka_topic


@register(singleton=True, name="candle_provider")
class CandleProvider(Service):

    @inject
    def __init__(self, config: Dict):
        super().__init__()
        self.config = config
        self.trading_pairs_consumer = AIOKafkaConsumer(
            config.kafka_trading_pairs_topic,
            loop=asyncio.get_event_loop(),
            bootstrap_servers=config.kafka_bootstrap_servers,
            auto_offset_reset="earliest",
        )
        self.trading_pairs = Dict()
        self.trading_pairs_queue: asyncio.Queue = asyncio.Queue(maxsize=10 ** 3)
        self.candles_consumer = AIOKafkaConsumer(
            loop=asyncio.get_event_loop(),
            bootstrap_servers=config.kafka_bootstrap_servers,
            auto_offset_reset="earliest",
            group_id=config.kafka_candles_consumer_group_id,
        )
        # aiokafka bug workaround
        # https://github.com/aio-libs/aiokafka/issues/536
        # https://github.com/aio-libs/aiokafka/pull/539
        # TODO: replace when fixed
        self._candle_subscriptions_present = asyncio.Event()

    @backoff.on_exception(
        backoff.expo,
        ConnectionError,
        max_time=5 * 60)
    async def on_start(self):
        await self.trading_pairs_consumer.start()
        self.add_future(self._consume_trading_pairs())
        self.add_future(self._subscribe_trading_pairs())

    async def on_stop(self):
        await self.trading_pairs_consumer.stop()

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
                self.candles_consumer.subscribe(topics=topics)
                self._candle_subscriptions_present.set()
                updated = False
            else:
                topic = normalize_kafka_topic(tp.topic)
                if topic in self.trading_pairs:
                    logger.debug("Update trading pair {} to {}", self.trading_pairs[topic], tp)
                else:
                    logger.debug("New trading pair {}", tp)
                self.trading_pairs[topic] = tp
                updated = True

    async def __aiter__(self):
        await self._candle_subscriptions_present.wait()
        logger.info("candle consumer starting...")
        try:
            await self.candles_consumer.start()
            logger.info("candle consumer started")
            async for message in self.candles_consumer:
                logger.debug("Candle message received: {}", message)
                try:
                    candle = Candle.from_json(message.value)
                except Exception:
                    logger.exception("Exception on candle parsing")
                else:
                    yield candle
        finally:
            await self.candles_consumer.stop()
