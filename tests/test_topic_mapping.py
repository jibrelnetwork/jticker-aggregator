import json
import random
import logging

import pytest

from aiokafka import AIOKafkaProducer

from jticker_aggregator.topic_mapping import TopicMappingConsumer
from jticker_aggregator.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
)


@pytest.mark.asyncio
async def test_topic_mapping(event_loop, caplog):
    rnd_suffix = random.randint(10 ** 4, 10 ** 5)
    tmp_assets_topic = f'assets_metadata_tests_{rnd_suffix}'

    mapping = TopicMappingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, loop=event_loop,
        mapping_topic=tmp_assets_topic
    )

    await mapping.start()

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        loop=event_loop,
        key_serializer=lambda x: x.encode()
    )
    await producer.start()

    await producer.send(
        topic=tmp_assets_topic,
        key=f'binance:BTCUSD',
        value=json.dumps({
            'exchange': 'binance',
            'symbol': 'BTCUSD',
            'topic': 'test_assets_topic'
        }).encode()
    )

    await producer.stop()

    async for trading_pair in mapping.available_trading_pairs():
        assert trading_pair
        assert trading_pair.exchange == 'binance'
        assert trading_pair.symbol == 'BTCUSD'
        assert trading_pair.topic == 'test_assets_topic'
        break
    else:
        pytest.fail("No trading pairs received from mapping")

    await mapping.stop()
