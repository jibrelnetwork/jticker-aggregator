import asyncio
import logging
from collections import Counter, defaultdict
from typing import Dict
from dataclasses import dataclass

from .candle import Candle

logger = logging.getLogger(__name__)


@dataclass
class TimeRange:
    start: str
    end: str


class AggregatorStats:

    """Aggregator stats.

    Collects and reports aggregator metrics.
    """

    #: number of seconds between stats messages in log
    log_interval: int

    #: count stored candles by exchange name
    by_exchange: Counter

    #: number of candles stored in series storage by storage instance
    by_series_instance: Counter

    #: bounding time interval for received candles
    timestamps_by_exchange: Dict[str, TimeRange]

    #: list of unique symbols received in candles since last stats reset
    unique_symbols: Dict[str, set]

    #: task to output stats
    _log_task: asyncio.Task

    def __init__(self, log_interval=60):
        self.by_exchange = Counter()
        self.by_series_instance = Counter()
        self.timestamps_by_exchange = {}
        self.unique_symbols = defaultdict(set)
        self.log_interval = log_interval

    def candle_stored(self, candle: Candle, series_instance: str):
        """Count stored candle.

        :param candle: stored Candle instance
        :param series_instance: id of series storage instance (host)
        :return:
        """
        self.by_exchange[candle.exchange] += 1
        self.by_series_instance[series_instance] += 1
        self.unique_symbols[candle.exchange].add(candle.symbol)
        self.add_timestamp(candle.exchange, candle.timestamp)

    def reset(self):
        """Reset stats after flush.
        """
        self.by_exchange.clear()
        self.by_series_instance.clear()
        self.unique_symbols.clear()
        self.timestamps_by_exchange.clear()

    def get_stats(self):
        """Return stats to output.
        """
        return {
            'by_exchange': self.by_exchange,
            'by_series_instance': self.by_series_instance,
            'unique_symbols': {
                exchange: len(symbols)
                for exchange, symbols in self.unique_symbols.items()
            },
            'timestamps': self.timestamps_by_exchange
        }

    def add_timestamp(self, exchange, timestamp):
        current = self.timestamps_by_exchange.get(exchange)

        if current is None:
            current = TimeRange(start=timestamp, end=timestamp)
            self.timestamps_by_exchange[exchange] = current
        else:
            # compare string to not add parsing overhead
            if timestamp < current.start:
                current.start = timestamp
            elif timestamp > current.end:
                current.end = timestamp

    async def start(self):
        loop = asyncio.get_event_loop()
        self._log_task = loop.create_task(self.log_task())

    async def stop(self):
        if hasattr(self, '_log_task'):
            self._log_task.cancel()
            await self._log_task

    async def log_task(self):
        while True:
            try:
                await asyncio.sleep(self.log_interval)
                logger.info("Aggregator stats", extra=self.get_stats())
                self.reset()
            except asyncio.CancelledError:
                break
            except:  # noqa
                logger.exception("Exception while logging stats")
