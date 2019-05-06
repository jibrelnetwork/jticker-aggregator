import json
import asyncio
import logging
from typing import Dict

from aiokafka import AIOKafkaConsumer
from async_timeout import timeout

from .candle import Candle


logger = logging.getLogger(__name__)


ASSETS_TOPIC = 'assets_metadata'


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

    #: map topic name to trading pair metadata received from ASSETS_TOPIC
    _topic_map: Dict[str, Dict]

    def __init__(self, *topics, **kwargs):
        """Candle consumer CTOR.

        :param topics: topics to consume
        :param kwargs: AIOKafkaConsumer kwargs
        """
        logger.debug("Subscribe to topics: %s", topics)
        self._topic_map = {}
        super().__init__(*topics, **kwargs)

    async def start(self):
        """Start consumer.

        Read assets topic to get quotes topics list.

        :return:
        """
        self.subscribe(await self.available_topics())

    async def available_topics(self):
        self.subscribe(topics=[ASSETS_TOPIC])

        await super().start()

        available_topics = []

        await self.seek_to_beginning()

        while True:
            try:
                # TODO: get max offset for partition and read messages before
                async with timeout(1.0):
                    msg = await self.getone()
                    data = json.loads(msg.value)
                    topic = data.get('topic')
                    if topic:
                        available_topics.append(topic)
                        self._topic_map[topic] = data
                    else:
                        logger.error("No kafka topic found: %s", data)
            except asyncio.TimeoutError:
                # all published assets received, break loop
                logger.debug("All published trading pairs loaded.")
                break

        logger.info("Topics loading complete. %i topics found.",
                    len(available_topics))
        logger.debug('Available topics: %s', available_topics)
        return available_topics

    def parse_candle(self, topic, data) -> Candle:
        """Create candle from Kafka message.

        :param topic: message origin topic
        :param data: message data
        :return:
        """
        spec = self._topic_map.get(topic, {})

        return Candle(
            exchange=spec['exchange'],
            symbol=spec['symbol'],
            # FIXME: no interval in assets metadata
            interval=int(spec.get('interval', 60)),
            timestamp=data.pop('time'),
            **data
        )
