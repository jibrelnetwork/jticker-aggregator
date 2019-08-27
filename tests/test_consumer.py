import time

import pytest

from jticker_aggregator.consumer import Consumer
from jticker_aggregator.candle import Candle
from jticker_aggregator.settings import KAFKA_BOOTSTRAP_SERVERS

from .utils import fill_kafka


example_candle_data = {
    'timestamp': time.time(),
    'open': 1,
    'high': 1.2,
    'low': 0.9,
    'close': 1.1,
    'interval': 60,
}


@pytest.mark.asyncio
async def test_iteration(event_loop):
    await fill_kafka({
        'assets_metadata': {
            'binance:BTCUSD': {
                'exchange': 'binance',
                'symbol': 'BTCUSD',
                'topic': 'binance_BTCUSD_60'
            }
        },
        'binance_BTCUSD_60': {
            'doesnt-matter': example_candle_data
        },
    })

    consumer = Consumer(
        loop=event_loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest'
    )

    await consumer.start()

    async for candle in consumer:
        assert isinstance(candle, Candle)
        for field in ('open', 'high', 'low', 'close', 'interval'):
            assert getattr(candle, field) == example_candle_data[field]
        break

    await consumer.stop()

    await event_loop.shutdown_asyncgens()
