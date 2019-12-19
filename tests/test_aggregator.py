import pytest

from jticker_core import TradingPair, Candle, Interval
from jticker_core.testing import async_condition


@pytest.mark.asyncio
async def test_successful_lifecycle(aggregator, mocked_kafka, config, timestamp_utc_minute_now,
                                    influx_client, wait_candles):
    tp = TradingPair(symbol="ab", exchange="ex")
    mocked_kafka.put(config.kafka_trading_pairs_topic, tp.as_json())
    await async_condition(lambda: len(mocked_kafka.subs) == 2)
    assert tp.topic in mocked_kafka.subs
    c = Candle(
        exchange="ex",
        symbol="ab",
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


@pytest.mark.asyncio
async def test_bad_trading_pair_format(aggregator, mocked_kafka, config):
    mocked_kafka.put(config.kafka_trading_pairs_topic, "bad string")
    tp = TradingPair(symbol="ETHBTC", exchange="ex")
    mocked_kafka.put(config.kafka_trading_pairs_topic, tp.as_json())
    await async_condition(lambda: len(mocked_kafka.subs) == 2)


@pytest.mark.asyncio
async def test_trading_pair_update(aggregator, mocked_kafka, config):
    tp = TradingPair(symbol="ETHBTC", exchange="ex")
    mocked_kafka.put(config.kafka_trading_pairs_topic, tp.as_json())
    await async_condition(lambda: len(mocked_kafka.subs) == 2)
    tp.symbol = "CHANGED"
    mocked_kafka.put(config.kafka_trading_pairs_topic, tp.as_json())
    tps = aggregator.candle_provider.trading_pairs
    await async_condition(lambda: "CHANGED" in {t.symbol for t in tps.values()})


@pytest.mark.asyncio
async def test_bad_candle_format(aggregator, mocked_kafka, config, timestamp_utc_minute_now,
                                 wait_candles):
    tp = TradingPair(symbol="ETHBTC", exchange="ex")
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
    good_candle = c.as_json()
    mocked_kafka.put(tp.topic, "bad string")
    c.high, c.low = c.low, c.high
    mocked_kafka.put(tp.topic, c.as_json())
    mocked_kafka.put(tp.topic, good_candle)
    cs = await wait_candles(aggregator.candle_consumer._time_series, tp)
    assert len(cs) == 1
    assert cs[0] == Candle.from_json(good_candle)


@pytest.mark.asyncio
async def test_stuck(not_started_aggregator, mocked_kafka, config):
    tp = TradingPair(symbol="ETHBTC", exchange="ex")
    mocked_kafka.put(config.kafka_trading_pairs_topic, tp.as_json())
    async with not_started_aggregator:
        await async_condition(lambda: not_started_aggregator.crashed)
