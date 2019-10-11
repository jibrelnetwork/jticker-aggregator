import asyncio
from collections import Counter, defaultdict
from typing import Dict

from prometheus_client import Counter as PrometheusCounter
from mode import Service
from loguru import logger

from jticker_core import Candle, inject, register


@register(singleton=True, name="aggregator_stats")
class AggregatorStats(Service):

    @inject
    def __init__(self, config):
        super().__init__()
        self.by_exchange: Counter = Counter()
        self.counter = PrometheusCounter(
            "aggregator_candles_total",
            "Jticker aggregator candles count",
            labelnames=("exchange", "symbol"),
        )
        self.unique_symbols: Dict[str, set] = defaultdict(set)
        self.log_interval: int = int(config.stats_log_interval)

    def _tr(self, s: str):
        return s.translate(self._translation)

    def candle_stored(self, candle: Candle):
        self.counter.labels(
            self._tr(candle.exchange),
            self._tr(candle.symbol),
        ).inc()
        self.by_exchange[candle.exchange] += 1
        self.unique_symbols[candle.exchange].add(candle.symbol)

    def reset(self):
        self.by_exchange.clear()
        self.unique_symbols.clear()

    def get_stats(self):
        return {
            "candles_stored": self.by_exchange,
            "unique_symbols": {
                exchange: len(symbols)
                for exchange, symbols in self.unique_symbols.items()
            }
        }

    async def on_start(self):
        self.add_future(self.log_task())

    async def log_task(self):
        while True:
            try:
                await asyncio.sleep(self.log_interval)
                stats = self.get_stats()
                logger.bind(stats=stats).info("Aggregator stats: {!r}", stats)
                self.reset()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Exception while logging stats")
