import pytest
import time
import asyncio

from jticker_core import TradingPair, Candle, Interval


@pytest.mark.asyncio
async def test_stats_propagation(aggregator, _aggregator_stats, mocked_kafka, config, condition,
                                 mocked_influx):
    tp = TradingPair(symbol="ETHBTC", exchange="ex")
    mocked_kafka.put(config.kafka_trading_pairs_topic, tp.as_json())
    await condition(lambda: len(mocked_kafka.subs) == 2)
    assert tp.topic in mocked_kafka.subs
    t = time.time()
    c = Candle(
        exchange="ex",
        symbol="ETHBTC",
        interval=Interval.MIN_1,
        timestamp=t,
        open=2,
        high=4,
        low=1,
        close=3,
    )
    mocked_kafka.put(tp.topic, c.as_json())
    await condition(lambda: len(mocked_influx._measurements) == 2)
    # awaiting log interval
    await asyncio.sleep(0.2)
