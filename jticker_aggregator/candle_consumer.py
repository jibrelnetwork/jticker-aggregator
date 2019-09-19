import asyncio
import uuid

import backoff
from mode import Service
from addict import Dict
from aiohttp import ClientError
from aioinflux import InfluxDBClient, InfluxDBError
from loguru import logger

from jticker_core import Candle, TradingPair, inject, register

from .stats import AggregatorStats


class SingleCandleConsumer(Service):

    @inject
    def __init__(self, config: Dict, host: str, version: str):
        super().__init__()
        self.config = config
        self.host = host
        self.version = version
        self.logger = logger.bind(prefix=host)
        self.client = InfluxDBClient(
            host=host,
            port=int(config.influx_port),
            db=config.influx_db,
            unix_socket=config.influx_unix_socket,
            ssl=bool(config.influx_ssl),
            username=config.influx_username,
            password=config.influx_password,
        )
        self.candle_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._measurement_mapping = None

    @backoff.on_exception(
        backoff.expo,
        (ClientError, InfluxDBError),
        max_time=5 * 60)
    async def on_start(self):
        await self._load_measurements()
        self.logger.info("loaded {} mapping values from influx", len(self._measurement_mapping))
        self.add_future(self._consume_candles())

    async def on_stop(self):
        await self.client.close()

    def _trading_pair_key(self, tp: TradingPair):
        return f"{tp.exchange}:{tp.symbol}"

    async def _load_measurements(self):
        """
        Load measurements mapping from special measurement.

        `{exchange}:{symbol}` key is used to identify trading pair in measurement
        name mapping.
        """
        self._measurement_mapping = {}
        resp = await self.client.query(f"SELECT * FROM {self.config.influx_measurements_mapping};")
        if "series" not in resp["results"][0]:
            # empty results, fresh mapping
            return
        series = resp["results"][0]["series"][0]
        result = [dict(zip(series["columns"], row)) for row in series["values"]]
        for item in result:
            tp = TradingPair(item["symbol"], item["exchange"])
            key = self._trading_pair_key(tp)
            self._measurement_mapping[key] = item["measurement"]

    @backoff.on_exception(
        backoff.constant,
        (ClientError, InfluxDBError),
        jitter=None,
        interval=1)
    async def _consume_candles(self):
        """
        Store candles from queue to influxdb.
        """
        while True:
            candle: Candle = await self.candle_queue.get()
            measurement = await self._get_measurement_by_candle(candle)
            influx_record = {
                "measurement": measurement,
                # precision is not supported by aioinflux
                # (https://github.com/gusutabopb/aioinflux/issues/25)
                "time": candle.time_iso8601,
                "tags": {
                    "interval": candle.interval.value,
                    "version": 0,
                    "aggregator_version": self.version,
                },
                "fields": {
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "base_volume": candle.base_volume,
                    "quote_volume": candle.quote_volume,
                }
            }
            await self.client.write(influx_record)
            self.candle_queue.task_done()

    async def _get_measurement_by_candle(self, candle: Candle):
        tp = TradingPair(exchange=candle.exchange, symbol=candle.symbol)
        key = self._trading_pair_key(tp)
        if self._measurement_mapping is None:
            raise RuntimeError("Measurements mapping is not loaded")
        if key not in self._measurement_mapping:
            measurement = f"{tp.exchange}_{tp.symbol}_{uuid.uuid4().hex}"
            await self.client.write({
                "measurement": self.config.influx_measurements_mapping,
                "tags": {
                    "aggregator_version": self.version,
                },
                "fields": {
                    "exchange": tp.exchange,
                    "symbol": tp.symbol,
                    "measurement": measurement,
                }
            })
            self._measurement_mapping[key] = measurement
            self.logger.info("add measurement mapping {!r}:{!r}", key, measurement)
        return self._measurement_mapping[key]


@register(singleton=True, name="candle_consumer")
class CandleConsumer(Service):

    @inject
    def __init__(self, config: Dict, aggregator_stats: AggregatorStats):
        super().__init__()
        self.aggregator_stats = aggregator_stats
        hosts = config.influx_host.split(",")
        self.consumers: list = [SingleCandleConsumer(host=host) for host in hosts]

    def on_init_dependencies(self):
        return self.consumers + [self.aggregator_stats]

    async def store_candle(self, candle: Candle):
        coros = [asyncio.wait_for(c.candle_queue.put(candle), 10) for c in self.consumers]
        results = await asyncio.gather(*coros, return_exceptions=True)
        for r in results:
            if isinstance(r, asyncio.TimeoutError):
                logger.opt(exception=r).error("Consumer queue is full")
        self.aggregator_stats.candle_stored(candle)
