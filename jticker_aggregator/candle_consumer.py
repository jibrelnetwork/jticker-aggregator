import asyncio

import backoff
from mode import Service
from addict import Dict
from aiohttp import ClientError
from aioinflux import InfluxDBClient, InfluxDBError
from loguru import logger

from jticker_core import Candle, TradingPair

from .injector import inject, register


class SingleCandleConsumer(Service):

    @inject
    def __init__(self, config: Dict, host: str):
        super().__init__()
        self.config = config
        self.host = host
        self.client = InfluxDBClient(
            host=host,
            port=int(config.influx_port),
            db=config.influx_db,
            unix_socket=config.influx_unix_socket,
            ssl=config.influx_ssl,
            username=config.influx_username,
            password=config.influx_password,
        )
        self.candle_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._measurement_mapping = None

        async def on_start(self):
            await self._load_measurements()
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
            resp = await self.client.query(f"SELECT * FROM {self.config.measurements_mapping};")
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
            backoff.constant(1),
            (ClientError, InfluxDBError),
            jitter=None)
        async def _consume_candles(self):
            """Store candles from queue to influxdb.
            """
            while True:
                candle = await self.candle_queue.get()
                measurement = await self._get_measurement_by_candle(candle)
                influx_record = {
                    "measurement": measurement,
                    "time": candle.timestamp,
                    "tags": {
                        "interval": candle.interval,
                        "version": 0
                    },
                    "fields": {
                        "open": float(candle.open),
                        "high": float(candle.high),
                        "low": float(candle.low),
                        "close": float(candle.close),
                        "base_volume": candle.base_volume,
                        "quote_volume": candle.quote_volume,
                    }
                }
                await self.client.write(influx_record, precision="s")
                # if self._stats is not None:
                #     self._stats.candle_stored(candle, self.client.host)
                self.candle_queue.task_done()

        async def _get_measurement_by_candle(self, candle):
            pass


@register(singleton=True, name="candle_consumer")
class CandleConsumer(Service):

    @inject
    def __init__(self, config: Dict):
        super().__init__()
        hosts = config.influx_host.split(",")
        self.consumers: list = [SingleCandleConsumer(host) for host in hosts]

    def on_init_dependencies(self):
        return self.consumers

    async def on_stop(self):
        logger.info("Awaiting queued candles stored to db for 10 seconds ...")
        coros = [asyncio.wait_for(c.candle_queue.join(), 10) for c in self.consumers]
        await asyncio.gather(*coros)

    async def store_candle(self, candle: Candle):
        coros = [asyncio.wait_for(c.candle_queue.put(candle), 10) for c in self.consumers]
        results = await asyncio.gather(*coros, return_exceptions=True)
        for r in results:
            if isinstance(r, asyncio.TimeoutError):
                logger.opt(exception=r).error("Consumer queue is full")


# import logging
# import asyncio
# from typing import List, Dict, Optional

# from aiohttp import ClientError
# from aioinflux import InfluxDBClient, InfluxDBError

# from .trading_pair import TradingPair
# from .stats import AggregatorStats
# from .candle import Candle


# logger = logging.getLogger(__name__)


# # name of measurement with trading pair -> measurement name mapping
# MAPPING_MEASUREMENT = 'mapping'


# def to_float(v):
#     return float(v) if v is not None else None


# class SeriesStorage:

#     """Candle series storage abstraction for aggregator.

#     Also it is used to load and update measurement name mapping
#     (trading pair -> measurement name).

#     Can be used as context manager:

#         async with SeriesStorage() as storage:
#             pass

#     or you should close session explicitly:

#         try:
#             storage = SeriesStorage()
#         finally:
#             storage.close()
#     """

#     #: is measurements name mapping loaded from MAPPING_MEASUREMENT
#     _measurements_loaded = False
#     #: map trading pair id to measurement name
#     _measurement_mapping: Dict[str, str]
#     #: candles buffer
#     _candles_buffer: asyncio.Queue
#     #: store candle task
#     _store_candles_task: asyncio.Task

#     _stats: Optional[AggregatorStats]

#     def __init__(self,
#                  host: str = "localhost",
#                  port: int = 8086,
#                  db_name: str = "jticker",
#                  ssl: bool = False,
#                  unix_socket: Optional[str] = None,
#                  username: Optional[str] = None,
#                  password: Optional[str] = None,
#                  loop=None,
#                  stats: AggregatorStats = None,
#                  **kwargs):
#         self._stats = stats
#         self.client = InfluxDBClient(
#             host=host,
#             port=port,
#             db=db_name,
#             unix_socket=unix_socket,
#             ssl=ssl,
#             username=username,
#             password=password,
#             **kwargs
#         )
#         self._measurement_mapping = {}
#         self.loop = loop or asyncio.get_event_loop()
#         self._candles_buffer = asyncio.Queue(maxsize=1000, loop=self.loop)

#     async def start(self):
#         self._store_candles_task = self.loop.create_task(self.store_candles_coro())

#     async def store_candle(self, candle: Candle):
#         """Store candle in influx measurement.

#         :param measurement: measurement name
#         :param candle: Candle instance
#         :return:
#         """

#         try:
#             await asyncio.wait_for(self._candles_buffer.put(candle), 10)
#             logger.debug("Candle added to buffer")
#         except asyncio.TimeoutError:
#             logger.warning("Candles buffer is full, can't store candle")

#     async def store_candles_coro(self):
#         """Store candles from buffer queue to influxdb.
#         """
#         while True:
#             try:
#                 candle = await self._candles_buffer.get()
#                 measurement = await self.get_measurement(candle.trading_pair)
#                 influx_record = {
#                     "measurement": measurement,
#                     "time": candle.timestamp,
#                     "tags": {
#                         "interval": candle.interval,
#                         "version": 0
#                     },
#                     "fields": {
#                         'open': float(candle.open),
#                         'high': float(candle.high),
#                         'low': float(candle.low),
#                         'close': float(candle.close),
#                         'base_volume': to_float(candle.base_volume),
#                         'quote_volume': to_float(candle.quote_volume),
#                     }
#                 }
#                 await self.client.write(influx_record)
#                 if self._stats is not None:
#                     self._stats.candle_stored(candle, self.client.host)
#                 self._candles_buffer.task_done()
#             except asyncio.CancelledError:
#                 logger.debug("store_candles_coro cancelled")
#                 break
#             except (ClientError, InfluxDBError):
#                 logger.exception("Exception happen while writting to InfluxDB."
#                                  "Sleep 1 sec and retry.")
#                 await asyncio.sleep(1)
#             except Exception:  # noqa
#                 logger.exception("Unhandled exception in store_candles_coro")
#                 await asyncio.sleep(10)
#                 exit(1)
#         logger.info("Store candle coro stopped.")

#     async def load_measurements_map(self):
#         """Load measurements mapping from special measurement.

#         `{exchange}:{symbol}` key is used to identify trading pair in measurement
#         name mapping.
#         """
#         resp = await self.client.query(f"SELECT * FROM {MAPPING_MEASUREMENT};")
#         if 'series' not in resp['results'][0]:
#             # empty results, fresh mapping
#             self._measurements_loaded = True
#             return
#         series = resp['results'][0]['series'][0]
#         result = [dict(zip(series['columns'], row)) for row in series['values']]
#         for item in result:
#             key = f"{item['exchange']}:{item['symbol']}"
#             self._measurement_mapping[key] = item['measurement']
#         self._measurements_loaded = True

#     async def get_measurement(self, trading_pair: TradingPair):
#         """Get current trading pair measurement.

#         If there is no measurement name for provided trading pair, then it will
#         be generated and stored.

#         :param trading_pair:
#         :return:
#         """
#         assert self._measurements_loaded, \
#             "You must load measurement mapping first using " \
#             "`load_measurement_map` method "
#         measurement = self._measurement_mapping.get(self._trading_pair_key(trading_pair))
#         if not measurement:
#             measurement = f"{trading_pair.exchange}_{trading_pair.symbol}_60"  # TODO: random suffix
#             await self.update_measurement_name(trading_pair, measurement)
#         return measurement

#     async def update_measurement_name(self, trading_pair, measurement):
#         await self.client.write({
#             "measurement": MAPPING_MEASUREMENT,
#             "fields": {
#                 "exchange": trading_pair.exchange,
#                 "symbol": trading_pair.symbol,
#                 "measurement": measurement,
#             }
#         })
#         trading_pair.measurement = measurement
#         self._measurement_mapping[self._trading_pair_key(trading_pair)] = measurement

#     async def __aenter__(self):
#         await self.start()
#         return self

#     async def __aexit__(self, exc_type, exc_val, exc_tb):
#         await self.close()

#     async def close(self):
#         self._store_candles_task.cancel()
#         await self.client.close()

#     def _trading_pair_key(self, trading_pair):
#         return f"{trading_pair.exchange}:{trading_pair.symbol}"


# class SeriesStorageSet:
#     """Series storage set.

#     Combine multiple series storage instances in a single one.
#     """
#     storage_instances: List[SeriesStorage]

#     def __init__(self, storage_instances):
#         self.storage_instances = storage_instances

#     async def load_measurements_map(self):
#         await self._gather_childs('load_measurements_map')

#     async def store_candle(self, candle: Candle):
#         await self._gather_childs(
#             'store_candle', candle=candle
#         )

#     async def start(self):
#         await self._gather_childs('start')

#     async def close(self):
#         await self._gather_childs('close')

#     async def _gather_childs(self, method_name, *args, **kwargs):
#         """Invoke series instances in parallel.

#         :param method_name: method to invoke
#         :param args: passed args
#         :param kwargs: passed kwargs
#         :return:
#         """
#         result = await asyncio.gather(
#             *[getattr(s, method_name)(*args, **kwargs)
#               for s in self.storage_instances],
#             return_exceptions=True
#         )
#         for item in result:
#             if isinstance(item, Exception):
#                 try:
#                     raise item
#                 except Exception:  # noqa
#                     logger.exception("Exception with one of storage")
