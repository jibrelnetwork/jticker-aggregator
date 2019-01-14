import json
import logging

from aiokafka import AIOKafkaConsumer

from .candle import Candle


logger = logging.getLogger(__name__)


class Consumer(AIOKafkaConsumer):

    """Candles consumer.

    Wrap kafka consumer: parse candles from received messages while iterating.

    TODO: we need to gather topics from meta api (all kafka topics gathered
        for a while)

    """

    def __init__(self, *topics, **kwargs):
        """Candle consumer CTOR.

        :param topics: topics to consume
        :param kwargs: AIOKafkaConsumer kwargs
        """
        logger.debug("Subscribe to topics: %s", topics)
        super().__init__(*topics, **kwargs)

    async def start(self):
        """Start consumer.

        Also read topics list and subscribe to all of them (FIXME)

        :return:
        """
        await super().start()
        logger.info("Loading available kafka topics...")
        available_topics = await self.topics()
        logger.info("Topics loading complete.")
        self.subscribe(topics=available_topics)

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
                return self.parse_candle(msg.topic, data)
            else:
                logger.error('Unhandled message type %s in %s Kafka topic: %s',
                             msg_type, msg.topic, data)

    def parse_candle(self, topic, data) -> Candle:
        """Create candle from Kafka message.

        :param topic: message origin topic
        :param data: message data
        :return:
        """
        exchange, symbol, interval = topic.split('_')

        return Candle(
            exchange=exchange,
            symbol=symbol,
            interval=int(interval),
            timestamp=data.pop('time'),
            **data
        )
