import time

import pytest

from jticker_core import TradingPair, Candle, Interval


@pytest.mark.asyncio
async def test_successful_lifecycle(aggregator, mocked_kafka, config, condition, mocked_influx):
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
        open=1,
        high=2,
        low=3,
        close=4,
    )
    mocked_kafka.put(tp.topic, c.as_json())
    await condition(lambda: len(mocked_influx._measurements) == 2)
    mapping = mocked_influx.get(config.influx_measurements_mapping)
    assert len(mapping) == 1
    name = mapping[0]["fields"]["measurement"]
    candles = mocked_influx.get(name)
    assert len(candles) == 1
    assert candles[0]["time"] == c.time_iso8601
    assert candles[0]["tags"]["interval"] == c.interval.value
    assert candles[0]["fields"]["open"] == c.open
    assert candles[0]["fields"]["high"] == c.high
    assert candles[0]["fields"]["low"] == c.low
    assert candles[0]["fields"]["close"] == c.close


@pytest.mark.asyncio
async def test_bad_trading_pair_format(aggregator, mocked_kafka, config, condition):
    mocked_kafka.put(config.kafka_trading_pairs_topic, "bad string")
    tp = TradingPair(symbol="ETHBTC", exchange="ex")
    mocked_kafka.put(config.kafka_trading_pairs_topic, tp.as_json())
    await condition(lambda: len(mocked_kafka.subs) == 2)


@pytest.mark.asyncio
async def test_trading_pair_update(aggregator, mocked_kafka, config, condition):
    tp = TradingPair(symbol="ETHBTC", exchange="ex")
    mocked_kafka.put(config.kafka_trading_pairs_topic, tp.as_json())
    await condition(lambda: len(mocked_kafka.subs) == 2)
    tp.symbol = "CHANGED"
    mocked_kafka.put(config.kafka_trading_pairs_topic, tp.as_json())
    tps = aggregator.candle_provider.trading_pairs
    await condition(lambda: "CHANGED" in {t.symbol for t in tps.values()})


@pytest.mark.asyncio
async def test_bad_candle_format(aggregator, mocked_kafka, config, condition, mocked_influx):
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
        open=1,
        high=2,
        low=3,
        close=4,
    )
    mocked_kafka.put(tp.topic, "bad string")
    mocked_kafka.put(tp.topic, c.as_json())
    await condition(lambda: len(mocked_influx._measurements) == 2)


@pytest.mark.asyncio
async def test_non_empty_influx(not_started_aggregator, mocked_influx, condition):
    mocked_influx.put(
        "FAKE_MAPPING_TABLE",
        {
            "measurement": "FAKE_MAPPING_TABLE",
            "fields": {
                "exchange": "ex",
                "symbol": "ETHBTC",
                "measurement": "ex_ETHBTC_f6a4deb449824668a817eb67b92cccd0",
            },
        }
    )
    async with not_started_aggregator as a:
        await condition(lambda: len(a.candle_consumer.consumers[0]._measurement_mapping) > 0)
