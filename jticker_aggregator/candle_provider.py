import asyncio
import json

import backoff
from mode import Service
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import ConnectionError
from addict import Dict
from loguru import logger

from jticker_core import (RawTradingPair, register, inject, normalize_kafka_topic, StuckTimeOuter,
                          Candle)


@register(singleton=True, name="candle_provider")
class CandleProvider(Service):

    @inject
    def __init__(self, config: Dict):
        super().__init__()
        self.config = config
        self.timeout = float(self.config.kafka_candles_stuck_timeout)
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
        self._candle_consumer_started = False
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
        await asyncio.wait_for(self.trading_pairs_consumer.start(), self.timeout)
        self.add_future(self._consume_trading_pairs())
        self.add_future(self._subscribe_trading_pairs())

    async def on_stop(self):
        await self.trading_pairs_consumer.stop()

    async def _consume_trading_pairs(self):
        async for message in StuckTimeOuter(self.trading_pairs_consumer, timeout=self.timeout):
            logger.debug("Trading pair message received: {}", message)
            try:
                tp = RawTradingPair.from_json(message.value)
            except Exception:
                continue
            else:
                await self.trading_pairs_queue.put(tp)

    async def _subscribe_trading_pairs(self):
        updated = False
        while True:
            try:
                tp = await asyncio.wait_for(self.trading_pairs_queue.get(),
                                            float(self.config.trading_pair_queue_timeout))
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
            if not self._candle_consumer_started:
                await asyncio.wait_for(self.candles_consumer.start(), self.timeout)
                self._candle_consumer_started = True
                logger.info("candle consumer started")
            async for message in StuckTimeOuter(self.candles_consumer, timeout=self.timeout):
                logger.debug("Candle message received: {}", message)
                try:
                    candle = Candle.from_json(message.value)
                except json.JSONDecodeError:
                    continue
                except Exception:
                    logger.exception("can't build candle from {}", message.value)
                else:
                    yield candle
        finally:
            await self.candles_consumer.stop()
