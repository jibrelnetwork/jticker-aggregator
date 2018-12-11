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


INFLUX_HOST = os.getenv('INFLUX_HOST', 'influxdb')
INFLUX_DB = os.getenv('INFLUX_DB', 'test')

FROM_TOPIC_START = bool(os.getenv('FROM_TOPIC_START', 'false') == 'true')

default_topics_file = os.path.join(
    os.path.dirname(__file__), './kafka_topics.txt'
)
TOPICS_FILE = os.getenv('TOPICS_FILE', default_topics_file)


async def consume():
    _configure_logging('INFO')

    topic_map = {}

    # TODO: change the way topics loaded
    # TODO: get ticker_id from api or other service linked with postgres
    # load topics from txt file

    with open(TOPICS_FILE) as fp:
        for line_n, topic in enumerate(fp.readlines()):
            topic = topic.strip()
            exchange, symbol, interval = topic.split('_')
            topic_map[topic] = {
                'exchange': exchange,
                'ticker_id': 'ticker_%i' % line_n,
                'interval': int(interval)
            }
    all_topics = topic_map.keys()
    consumer = AIOKafkaConsumer(
        *all_topics,
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="aggregator",
    )

    logger.info("Starting consumer")
    logger.debug("Subscribe to topics: %s", topic_map.keys())

    await consumer.start()

    if FROM_TOPIC_START:
        await consumer.seek_to_beginning()

    try:
        # Consume messages
        async with InfluxDBClient(host=INFLUX_HOST, db=INFLUX_DB) as client:

            async for msg in consumer:
                logger.debug('Msg received from Kafka %s', msg)
                data = json.loads(msg.value)

                topic_info = topic_map[msg.topic]

                influx_record = {
                    "measurement": topic_info['ticker_id'],
                    "time": data['time'],
                    "tags": {
                        "interval": topic_info['interval'],
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
