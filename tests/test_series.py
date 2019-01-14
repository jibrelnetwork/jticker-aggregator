import time

import pytest
from asynctest import mock

from jticker_aggregator.series import SeriesStorage
from jticker_aggregator.candle import Candle


@pytest.mark.asyncio
async def test_candle_written():
    candle = Candle(
        'binance', 'BTCUSD', 60,
        timestamp=int(time.time()),
        open=1.1,
        high=2.1,
        low=.5,
        close=0.9,
        base_volume=1,
        quote_volume=2
    )
    async with SeriesStorage() as storage:
        storage.client.write = mock.CoroutineMock()
        await storage.store_candle('test_measurement', candle)
