import logging
import asyncio

import sentry_sdk

from .consumer import Consumer
from .series import SeriesStorage, SeriesStorageSet
from .metadata import Metadata, TradingPairNotExist
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
    METADATA_URL,
    LOG_LEVEL,
)


logger = logging.getLogger('jticker_aggregator')

loop = asyncio.get_event_loop()


if SENTRY_DSN:
    with open('version.txt', 'r') as fp:
        sentry_sdk.init(SENTRY_DSN, release=fp.read().strip())


metadata = Metadata(service_url=METADATA_URL)


async def consume():
    _configure_logging(LOG_LEVEL)

    influx_instances = []

    for host in INFLUX_HOST.split(','):
        influx_instances.append(SeriesStorage(
            host=host,
            db_name=INFLUX_DB,
            port=INFLUX_PORT,
            ssl=INFLUX_SSL,
            unix_socket=INFLUX_UNIX_SOCKET,
            username=INFLUX_USERNAME,
            password=INFLUX_PASSWORD,
            loop=loop
        ))

    storage = SeriesStorageSet(influx_instances)

    await storage.load_measurements_map()
    await storage.start()

    try:

        consumer = Consumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="aggregator",
            loop=loop,
        )

        await consumer.start()

        async for candle in consumer:
            try:
                trading_pair = await metadata.get_trading_pair(
                    exchange=candle.exchange, symbol=candle.symbol
                )
                await storage.store_candle(trading_pair, candle)
            except TradingPairNotExist:
                logger.debug("Skip candle because trading pair doesn't exist")
    except:  # noqa
        sentry_sdk.capture_exception()
        logger.exception("Exit on unhandled exception")
    finally:
        logger.info("Stopping consumer (finally)")
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


loop.run_until_complete(consume())
