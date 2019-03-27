import os
import logging
import asyncio

from .consumer import Consumer
from .series import SeriesStorage
from .metadata import Metadata
from .logging import _configure_logging


logger = logging.getLogger('jticker_aggregator')

loop = asyncio.get_event_loop()


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

INFLUX_HOST = os.getenv('INFLUX_HOST', 'influxdb')
INFLUX_PORT = int(os.getenv('INFLUX_PORT', '8086'))
INFLUX_DB = os.getenv('INFLUX_DB', 'test')
INFLUX_USERNAME = os.getenv('INFLUX_USERNAME')
INFLUX_PASSWORD = os.getenv('INFLUX_PASSWORD')
INFLUX_SSL = bool(os.getenv('INFLUX_SSL', 'false') == 'true')
INFLUX_UNIX_SOCKET = os.getenv('INFLUX_UNIX_SOCKET')

METADATA_URL = os.getenv('METADATA_URL', 'http://meta:8000/')


metadata = Metadata(service_url=METADATA_URL)
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
    _configure_logging('DEBUG')

    consumer = Consumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="aggregator",
        loop=loop,
    )

    await storage.load_measurements_map()

    await consumer.start()

    async for candle in consumer:
        try:
            trading_pair = await metadata.get_trading_pair(
                exchange=candle.exchange, symbol=candle.symbol
            )
            measurement = await storage.get_measurement(trading_pair)
            await storage.store_candle(measurement, candle)
        except:  # noqa
            logger.exception(
                "Exception happen while consuming candles from Kafka %s", candle
            )

    logger.info("Stopping consumer (finally)")
    # Will leave consumer group; perform autocommit if enabled.
    await consumer.stop()

loop.run_until_complete(consume())
