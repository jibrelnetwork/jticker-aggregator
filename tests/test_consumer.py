import time
import json

import pytest
from contextlib import contextmanager
from asynctest import mock

from jticker_aggregator.consumer import Consumer
from jticker_aggregator.candle import Candle


class FakeKafkaMessage:
    def __init__(self, topic, value):
        self.topic = topic
        self.value = json.dumps(value)


example_candle_data = {
    'time': time.time(),
    'open': 1,
    'high': 1.2,
    'low': 0.9,
    'close': 1.1,
}

example_kafka_message = FakeKafkaMessage(
    'binance_USDBTC_60',
    example_candle_data
)


@contextmanager
def consumer_patch(patch_message=None):
    if patch_message is None:
        patch_message = example_kafka_message

    consumer_patch_spec = {
        'aiokafka.consumer.consumer.AIOKafkaClient.bootstrap': {},
        'aiokafka.consumer.consumer.AIOKafkaClient.get_random_node': {
            'return_value': 1234,
        },
        'aiokafka.consumer.consumer.AIOKafkaClient._metadata_update': {
            'return_value': True,
        },
        'aiokafka.consumer.consumer.AIOKafkaConsumer.topics': {
            'new': mock.CoroutineMock(return_value=['abc']),
        },
        'aiokafka.consumer.consumer.AIOKafkaConsumer.__anext__': {
            'new': mock.CoroutineMock(return_value=patch_message),
        }
    }

    applied_pathes = []
    for target, spec in consumer_patch_spec.items():
        patcher = mock.patch(target, **spec)
        patcher.start()
        applied_pathes.append(
            patcher
        )
    yield
    for patcher in applied_pathes:
        patcher.stop()


@pytest.mark.asyncio
async def test_iteration(event_loop):
    with consumer_patch():
        consumer = Consumer(
            loop=event_loop,
            bootstrap_servers=('kafka:9092',),
            group_id="aggregator",
        )

        await consumer.start()

        async for candle in consumer:
            assert isinstance(candle, Candle)
            for field in ('open', 'high', 'low', 'close'):
                assert getattr(candle, field) == example_candle_data[field]
            break
