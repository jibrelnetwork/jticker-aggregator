import asyncio
from typing import List

import backoff
from addict import Dict
from mode import Service

from jticker_core import AbstractTimeSeriesStorage, Candle, TimeSeriesException, inject, register

from .stats import AggregatorStats


@register(singleton=True, name="candle_consumer")
class CandleConsumer(Service):

    @inject
    def __init__(self, config: Dict, time_series: AbstractTimeSeriesStorage,
                 aggregator_stats: AggregatorStats):
        super().__init__()
        self.config = config
        self._time_series_chunk_size = int(self.config.time_series_chunk_size)
        self._time_series_chunk: List[Candle] = []
        self._time_series = time_series
        self._stats = aggregator_stats

    def on_init_dependencies(self):
        return [
            self._time_series,
            self._stats,
        ]

    async def on_start(self):
        self.add_future(self._store_candles())

    async def on_stop(self):
        while self._time_series_chunk:
            await asyncio.sleep(0.1)

    @backoff.on_exception(
        backoff.constant,
        TimeSeriesException,
        jitter=None,
        interval=1)
    async def _store_candles(self):
        while True:
            while True:
                await asyncio.sleep(0.25)
                if self._time_series_chunk:
                    break
            chunk, self._time_series_chunk = self._time_series_chunk, []
            await self._time_series.add_candles(chunk)

    def should_wait(self):
        return len(self._time_series_chunk) > self._time_series_chunk_size

    async def store_candle(self, candle: Candle):
        self._time_series_chunk.append(candle)
        while self.should_wait():
            await asyncio.sleep(0.1)
        self._stats.candle_stored(candle)
