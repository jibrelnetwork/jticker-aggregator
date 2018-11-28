import os
import json
import logging
import asyncio

from aiokafka import AIOKafkaConsumer
from aioinflux import InfluxDBClient

from .logging import _configure_logging


logger = logging.getLogger('jticker_aggregator')

loop = asyncio.get_event_loop()


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

QUOTE_TOPIC = os.getenv('KAFKA_QUOTE_TOPIC', 'example_symbol_1m')

INFLUX_HOST = os.getenv('INFLUX_HOST', 'influxdb')
INFLUX_DB = os.getenv('INFLUX_DB', 'test')


async def consume():
    _configure_logging('DEBUG')

    consumer = AIOKafkaConsumer(
        QUOTE_TOPIC,
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="aggregator"
    )
    logger.info("Starting consumer")

    await consumer.start()

    try:
        # Consume messages
        async with InfluxDBClient(host=INFLUX_HOST, db=INFLUX_DB) as client:
            async for msg in consumer:
                data = json.loads(msg.value)
                influx_record = {
                    "measurement": "ticker_0",
                    "time": data['time'],
                    "tags": {
                        "interval": data['interval'],
                        "version": 0
                    },
                    "fields": {
                        k: float(v) for k, v in data.items() if k in {
                            'open', 'close', 'high', 'low', 'volume'
                        }
                    }
                }
                logger.debug("Write to influx: %s", influx_record)
                await client.write(influx_record)
    except Exception as e:
        logger.exception("Exception happen %s", e)
    finally:
        logger.info("Stopping consumer (finally)")
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


loop.run_until_complete(consume())
