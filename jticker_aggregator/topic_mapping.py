import json
import logging
from collections import defaultdict
from typing import Dict, AsyncGenerator

from aiokafka import AIOKafkaConsumer
from kafka import TopicPartition

from jticker_aggregator.trading_pair import TradingPair


logger = logging.getLogger(__name__)

ASSETS_TOPIC = 'assets_metadata'


class TopicMappingConsumer(AIOKafkaConsumer):

    """Topic mapping consumer.

    Allows to get actual topic names for trading pairs from mapping topic.

    Mapping topic consists of the messages in the following format:

        {
            "exchange": "bitfinex",
            "symbol": "tBTCUSD",
            "topic": "bitfinex_tBTCUSD_60",
            "interval": 60  # optional
        }

    New messages in the topic will override previous state of the trading pair
    (exchange-symbol). Consumer will yield updates as they published in topic.
    """

    def __init__(self, *args, mapping_topic=ASSETS_TOPIC, **kwargs):
        self.mapping_topic = mapping_topic
        kwargs['auto_offset_reset'] = 'earliest'
        super().__init__(*args, **kwargs)

    async def available_trading_pairs(self) -> AsyncGenerator[TradingPair, None]:
        """Iterate over trading pairs in topic mapping and wait for new updates.

        Read trading pairs updates from metadata topic to the actual end and
        yield final pairs state (skipping old topics to prevent subscription
        to them).

        After semi-final state yielded, wait for updates and yield them as a new
        pairs (application should handle this).
        """
        # trading pairs state should be accumulated to its final state before
        # yielding to prevent subscription to stale topics.
        accumulate = True

        # accumulated trading pairs (before end offset)
        acc_pairs: Dict[str, Dict[str, TradingPair]] = defaultdict(dict)

        # assume only one partition exist
        partition = TopicPartition(self.mapping_topic, 0)

        last_known_offset = (await self.end_offsets([partition]))[partition]

        self.assign([partition])

        await self.seek_to_beginning(partition)

        async for message in super().__aiter__():
            logger.debug("State update received: %s", message)
            data = json.loads(message.value)

            trading_pair = acc_pairs[data['exchange']].get(data['symbol'])
            if not trading_pair:
                trading_pair = TradingPair(
                    exchange=data['exchange'],
                    symbol=data['symbol'],
                    topic=data['topic']
                )
            else:
                trading_pair.topic = data['topic']

            if not accumulate:
                yield trading_pair
                continue

            acc_pairs[trading_pair.exchange][trading_pair.symbol] = trading_pair

            # stop accumulation if current position exceed last end offset
            current_position = await self.position(partition)
            if accumulate and current_position >= last_known_offset:
                logger.debug("Initial metadata loaded. %s", acc_pairs)
                for exchange_pairs in acc_pairs.values():
                    for trading_pair in exchange_pairs.values():
                        yield trading_pair
                accumulate = False
