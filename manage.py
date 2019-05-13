#!/usr/bin/env python3
import time
import asyncio
import logging
from datetime import datetime, timedelta

import click

from jticker_aggregator.checker import RangeCollection
from jticker_aggregator.consumer import Consumer


@click.group()
def main():
    pass


async def fill_collection(consumer, collection):
    start_time = time.time()
    click.echo("fill collection")
    await consumer.start()

    # get last offset
    partitions = list(consumer.assignment())
    end_offsets = await consumer.end_offsets(partitions)
    end = list(end_offsets.values())[0]
    current_pos = await consumer.position(partitions[0])

    click.echo("Finish offset %i" % end)
    click.echo("Start offset %i" % current_pos)

    if end == current_pos:
        click.echo("Topic is empty")
        await consumer.stop()
        return

    i = 0
    async for candle in consumer:
        value = datetime.strptime(candle.timestamp, '%Y-%m-%dT%H:%M:%S')
        collection.add_value(value.timestamp())  # FIXME: timestamp must be int
        if await consumer.position(partitions[0]) >= end - 1:
            logging.info("Found the end offset %s", end)
            break
        i += 1
        if i % 1000 == 0:
            click.echo("1000 candles processed")

    click.echo("Total of %i candles processed for %s secs" % (
               i, time.time() - start_time))

    await consumer.stop()


@main.command()
@click.argument('topic')
def check(topic):
    loop = asyncio.get_event_loop()
    consumer = Consumer(topic, loop=loop, bootstrap_servers='kafka:9092',
                        auto_offset_reset='earliest')
    collection = RangeCollection()
    loop.run_until_complete(fill_collection(consumer, collection))

    last = None
    for period in collection.ranges:
        if last:
            start = (
                datetime.fromtimestamp(last.end) + timedelta(minutes=1)
            ).isoformat()
            end = datetime.fromtimestamp(period.start).isoformat()
            click.echo("Hole: %s — %s" % (start, end))
        last = period

    click.echo("Finish")


if __name__ == '__main__':
    main()
