import random

import pytest

from jticker_aggregator.topic_mapping import TopicMappingConsumer
from jticker_aggregator.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
)

from .utils import fill_kafka


@pytest.mark.asyncio
async def test_topic_mapping(event_loop, caplog):
    rnd_suffix = random.randint(10 ** 4, 10 ** 5)
    tmp_assets_topic = f'assets_metadata_tests_{rnd_suffix}'

    await fill_kafka({
        tmp_assets_topic: {
            'binance:BTCUSD': {
                'exchange': 'binance',
                'symbol': 'BTCUSD',
                'topic': 'test_assets_topic'
            }
        }
    })

    mapping = TopicMappingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, loop=event_loop,
        mapping_topic=tmp_assets_topic
    )

    await mapping.start()

    async for trading_pair in mapping.available_trading_pairs():
        assert trading_pair
        assert trading_pair.exchange == 'binance'
        assert trading_pair.symbol == 'BTCUSD'
        assert trading_pair.topic == 'test_assets_topic'
        break
    else:
        pytest.fail("No trading pairs received from mapping")

    await mapping.stop()

    await event_loop.shutdown_asyncgens()
