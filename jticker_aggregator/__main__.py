import logging
import asyncio

import sentry_sdk

from .consumer import Consumer
from .series import SeriesStorage
from .trading_pair import TradingPair
from .logging import _configure_logging

from .settings import (
    SENTRY_DSN,
    KAFKA_BOOTSTRAP_SERVERS,
    INFLUX_HOST,
    INFLUX_PORT,
    INFLUX_USERNAME,
    INFLUX_PASSWORD,
    INFLUX_UNIX_SOCKET,
    INFLUX_SSL,
    INFLUX_DB,
    LOG_LEVEL,
)


logger = logging.getLogger('jticker_aggregator')

loop = asyncio.get_event_loop()


if SENTRY_DSN:
    with open('version.txt', 'r') as fp:
        sentry_sdk.init(SENTRY_DSN, release=fp.read().strip())

storage = SeriesStorage(
    host=INFLUX_HOST,
    db_name=INFLUX_DB,
    port=INFLUX_PORT,
    ssl=INFLUX_SSL,
    unix_socket=INFLUX_UNIX_SOCKET,
    username=INFLUX_USERNAME,
    password=INFLUX_PASSWORD
)


async def consume():
    _configure_logging(LOG_LEVEL)

    consumer = Consumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="aggregator",
        loop=loop,
    )

    try:
        await storage.load_measurements_map()

        await consumer.start()

        async for candle in consumer:
            trading_pair = TradingPair(exchange=candle.exchange,
                                       symbol=candle.symbol)
            measurement = await storage.get_measurement(trading_pair)
            await storage.store_candle(measurement, candle)
    except:  # noqa
        sentry_sdk.capture_exception()
        logger.exception("Exit on unhandled exception")
    finally:
        logger.info("Stopping consumer (finally)")
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


loop.run_until_complete(consume())
