import json
import asyncio
import logging
from typing import Dict

from aiokafka import AIOKafkaConsumer

from .topic_mapping import TopicMappingConsumer
from .candle import Candle
from .trading_pair import TradingPair


logger = logging.getLogger(__name__)


class CandleConsumer(AIOKafkaConsumer):

    """Consume assets and trading pairs metadata
    """

    async def __anext__(self):
        """Receive message, parse candle and yield it.

        :return:
        """
        while True:
            msg = await super().__anext__()
            data = json.loads(msg.value)
            msg_type = data.pop('type', 'candle')
            logger.debug('Msg received from Kafka (%s): %s', msg_type, msg)
            if msg_type == 'candle':
                try:
                    return self.parse_candle(msg.topic, data)
                except:  # noqa
                    logger.exception("Can't parse candle from message %s", msg)
            else:
                logger.error('Unhandled message type %s in %s Kafka topic: %s',
                             msg_type, msg.topic, data)

    def parse_candle(self, topic, data) -> Candle:
        """Create candle from Kafka message.

        :param topic: message origin topic
        :param data: message data
        :return:
        """

        return Candle(
            exchange=None,
            symbol=None,
            # FIXME: no interval in assets metadata
            interval=60,
            timestamp=data.pop('time'),
            **data
        )


class Consumer(CandleConsumer):

    """Candles consumer.

    Wrap kafka consumer: parse candles from received messages while iterating.
    """

    #: mapper used to gather all available topics
    _topic_mapping: TopicMappingConsumer

    #: map topic name to TradingPair
    _topic_map: Dict[str, TradingPair]
    #: reverse map (TradingPair -> topic name)
    _reverse_topic_map: Dict[TradingPair, str]

    _subscribe_task: asyncio.Task

    def __init__(self, *topics, **kwargs):
        """Candle consumer CTOR.
        """
        super().__init__(*topics, **kwargs)
        kwargs['group_id'] = None
        self._topic_mapping = TopicMappingConsumer(**kwargs)

        self._topic_map = {}
        self._reverse_topic_map = {}

    async def start(self):
        """Start consumer.

        Read assets topic to get quotes topics list.

        :return:
        """
        await self._topic_mapping.start()
        await super().start()

        loop = asyncio.get_event_loop()
        self._subscribe_task = loop.create_task(self.subscribe_task())

    async def subscribe_task(self):
        """Subscribe topics as they appearing in mapping.
        """
        try:
            async for trading_pair in self._topic_mapping.available_trading_pairs():
                topic = trading_pair.topic
                logger.debug("New mapping item received for %s", trading_pair)
                if self._reverse_topic_map.get(trading_pair):
                    logger.debug("Mapping for %s already exist, replacing...",
                                 trading_pair)
                    old_topic = self._reverse_topic_map[trading_pair]
                    del self._topic_map[old_topic]
                self._topic_map[topic] = trading_pair
                self._reverse_topic_map[trading_pair] = topic
                # TODO: there is very high frequency of calls on startup
                logger.debug("New subscription list: %s", self._topic_map.keys())
                self.subscribe(topics=list(self._topic_map.keys()))
        except:  # noqa
            logger.exception("Unhandled exception while subscribe")

    def parse_candle(self, topic, data) -> Candle:
        """Create candle from Kafka message.

        :param topic: message origin topic
        :param data: message data
        :return:
        """
        spec = self._topic_map.get(topic)

        if spec is None:
            raise Exception("No topic %s in mapping" % topic)

        return Candle(
            exchange=spec.exchange,
            symbol=spec.symbol,
            # FIXME: no interval in assets metadata
            interval=60,
            timestamp=data.pop('time'),
            **data
        )
