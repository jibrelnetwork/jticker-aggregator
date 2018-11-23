import os
import logging
import asyncio

from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

loop = asyncio.get_event_loop()


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

QUOTE_TOPIC = os.getenv('KAFKA_QUOTE_TOPIC', 'example_symbol_1m')


async def consume():
    consumer = AIOKafkaConsumer(
        QUOTE_TOPIC,
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="aggregator"
    )
    logger.info("Starting consumer")
    # Get cluster layout and join group `my-group`
    await consumer.start()
    logger.info("Consumer started")

    try:
        # Consume messages
        async for msg in consumer:
            logger.info("consumed: %s, %s, %s, %s, %s, %s", msg.topic, msg.partition, msg.offset,
                        msg.key, msg.value, msg.timestamp)
    except Exception as e:
        logger.exception("Exception happen %s", e)
    finally:
        logger.info("Stopping consumer (finally)")
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

loop.create_task(consume())
loop.run_forever()
