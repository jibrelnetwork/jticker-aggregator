import pytest
from asynctest import mock
from datetime import datetime

from jticker_aggregator.series import SeriesStorage
from jticker_aggregator.candle import Candle


@pytest.mark.asyncio
async def test_candle_written():
    candle = Candle(
        'binance', 'BTCUSD', 60,
        timestamp=datetime.now().isoformat(),
        open=1.1,
        high=2.1,
        low=.5,
        close=0.9,
        base_volume=1,
        quote_volume=2
    )

    async with SeriesStorage() as storage:
        storage._measurement_mapping['binance:BTCUSD'] = 'test_measurement'
        storage._measurements_loaded = True
        storage.client.write = mock.CoroutineMock()
        await storage.store_candle(candle)
