import pytest
import asyncio

from jticker_core import RawTradingPair, Candle, Interval
from jticker_core.testing import async_condition


@pytest.mark.asyncio
async def test_stats_propagation(aggregator, _aggregator_stats, mocked_kafka, config,
                                 timestamp_utc_minute_now, wait_candles):
    tp = RawTradingPair(symbol="ETHBTC", exchange="ex")
    mocked_kafka.put(config.kafka_trading_pairs_topic, tp.as_json())
    await async_condition(lambda: len(mocked_kafka.subs) == 2)
    assert tp.topic in mocked_kafka.subs
    c = Candle(
        exchange="ex",
        symbol="ETHBTC",
        interval=Interval.MIN_1,
        timestamp=timestamp_utc_minute_now,
        open=2,
        high=4,
        low=1,
        close=3,
    )
    mocked_kafka.put(tp.topic, c.as_json())
    cs = await wait_candles(aggregator.candle_consumer._time_series, tp)
    assert len(cs) == 1
    assert cs[0] == c
    # awaiting log interval
    await asyncio.sleep(0.2)
