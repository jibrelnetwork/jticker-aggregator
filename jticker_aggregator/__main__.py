import os
import json
import logging
import asyncio

from aiokafka import AIOKafkaConsumer
from aioinflux import InfluxDBClient

from .consumer import Consumer
from .series import SeriesStorage
from .metadata import Metadata
from .logging import _configure_logging


logger = logging.getLogger('jticker_aggregator')

loop = asyncio.get_event_loop()


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')


INFLUX_HOST = os.getenv('INFLUX_HOST', 'influxdb')
INFLUX_DB = os.getenv('INFLUX_DB', 'test')


metadata = Metadata()
storage = SeriesStorage(host=INFLUX_HOST, db_name=INFLUX_DB)


async def consume():
    _configure_logging('DEBUG')

    consumer = Consumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="aggregator",
        loop=loop,
    )

    try:
        await consumer.start()

        async for candle in consumer:
            trading_pair = await metadata.get_trading_pair(
                exchange=candle.exchange, symbol=candle.symbol
            )
            await storage.store_candle(trading_pair.measurement, candle)
    except Exception as e:
        logger.exception(
            "Exception happen while consuming candles from Kafka %s", e
        )
    finally:
        logger.info("Stopping consumer (finally)")
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

loop.run_until_complete(consume())
