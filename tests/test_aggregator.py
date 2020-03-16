import asyncio

import pytest

from jticker_core import Candle, Interval, Order, RawTradingPair


@pytest.mark.asyncio
async def test_successful_lifecycle(not_started_aggregator, timestamp_utc_minute_now):
    tp = RawTradingPair(exchange="ex", symbol="ab")
    c1 = Candle(
        exchange="ex",
        symbol="ab",
        interval=Interval.MIN_1,
        timestamp=timestamp_utc_minute_now,
        open=2,
        high=4,
        low=1,
        close=3,
    )
    c2 = Candle(
        exchange="ex",
        symbol="ab",
        interval=Interval.MIN_1,
        timestamp=timestamp_utc_minute_now + 60,
        open=2,
        high=4,
        low=1,
        close=3,
    )
    async with not_started_aggregator.stream_storage as st:
        await st.add_candles([c1, c1, c1, c2, c2], flush=True)
    async with not_started_aggregator as ag:
        await asyncio.sleep(0.1)
        cs = await ag.time_series.get_candles(tp, order=Order.ASC)
        assert len(cs) == 2
        assert (cs[0], cs[1]) == (c1, c2)
