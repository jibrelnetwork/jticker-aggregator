import os
import json
import logging
import asyncio

from aiokafka import AIOKafkaConsumer
from aioinflux import InfluxDBClient

from .metadata import Metadata
from .logging import _configure_logging


logger = logging.getLogger('jticker_aggregator')

loop = asyncio.get_event_loop()


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')


INFLUX_HOST = os.getenv('INFLUX_HOST', 'influxdb')
INFLUX_DB = os.getenv('INFLUX_DB', 'test')

default_topics_file = os.path.join(
    os.path.dirname(__file__), './kafka_topics.txt'
)
TOPICS_FILE = os.getenv('TOPICS_FILE', default_topics_file)


metadata = Metadata()


async def consume():
    _configure_logging('DEBUG')

    topic_map = {}

    consumer = AIOKafkaConsumer(
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="aggregator",
    )

    await consumer.start()

    logger.info("Loading new topics")
    available_topics = await consumer.topics()

    failed_topics = []
    for topic in available_topics:
        try:
            logger.debug("Topic %s found", topic)
            exchange, symbol, interval = topic.split('_')
            trading_pair = await metadata.sync_trading_pair(exchange, symbol)
            logger.debug("Topic trading pair: %s", trading_pair)
            topic_map[topic] = trading_pair
        except Exception:
            failed_topics.append(topic)
            logger.exception("Can't process topic %s (skip)", topic)

    logger.info('%i topics loaded', len(topic_map))

    await consumer.stop()

    logger.info("Consumer stopped before subscribe.")

    consumer = AIOKafkaConsumer(
        *available_topics,
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="aggregator",
    )

    await consumer.start()

    logger.debug("Subscribe to topics: %s", available_topics)

    try:
        # Consume messages
        async with InfluxDBClient(host=INFLUX_HOST, db=INFLUX_DB) as client:
            logger.info("Client acquired")
            async for msg in consumer:
                logger.debug('Msg received from Kafka %s', msg)
                data = json.loads(msg.value)

                exchange, symbol, interval = msg.topic.split('_')

                topic_info = topic_map[msg.topic]

                influx_record = {
                    "measurement": topic_info.measurement,
                    "time": data['time'],
                    "tags": {
                        "interval": int(interval),
                        "version": 0
                    },
                    "fields": {
                        k: float(v) for k, v in data.items() if k in {
                            'open', 'close', 'high', 'low', 'quote_volume'
                        } and v is not None
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
