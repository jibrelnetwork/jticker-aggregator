import json
import asyncio

from aiokafka import AIOKafkaProducer

from jticker_aggregator.settings import KAFKA_BOOTSTRAP_SERVERS


async def fill_kafka(payload):
    """Fill Kafka with data.
    Payload example:
        {
            "<TOPIC_NAME>": {
                "MSG-1-KEY": {
                    "some-json-serializable": "here"
                }
            },
            ...
        }
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        loop=asyncio.get_event_loop(),
        key_serializer=lambda x: x.encode()
    )
    await producer.start()

    for topic_name in payload:
        for key, value in payload[topic_name].items():
            await producer.send(
                topic=topic_name,
                key=key,
                value=json.dumps(value).encode()
        )

    await producer.stop()
