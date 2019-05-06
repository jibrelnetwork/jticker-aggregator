import json
import logging
import asyncio
from typing import List, Dict, Optional

from aiohttp import ClientError
from aioinflux import InfluxDBClient

from .metadata import TradingPair
from .candle import Candle


logger = logging.getLogger(__name__)


# name of measurement with trading pair -> measurement name mapping
MAPPING_MEASUREMENT = 'mapping'


def to_float(v):
    return float(v) if v is not None else None


class SeriesStorage:

    """Candle series storage abstraction for aggregator.

    Also it is used to load and update measurement name mapping
    (trading pair -> measurement name).

    Can be used as context manager:

        async with SeriesStorage() as storage:
            pass

    or you should close session explicitly:

        try:
            storage = SeriesStorage()
        finally:
            storage.close()
    """

    #: is measurements name mapping loaded from MAPPING_MEASUREMENT
    _measurements_loaded = False
    #: map trading pair id to measurement name
    _measurement_mapping: Dict[str, str]
    #: candles buffer
    _candles_buffer: asyncio.Queue
    #: store candle task
    _store_candles_task: asyncio.Task

    def __init__(self,
                 host: str = "localhost",
                 port: int = 8086,
                 db_name: str = "jticker",
                 ssl: bool = False,
                 unix_socket: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 loop=None,
                 **kwargs):
        self.client = InfluxDBClient(
            host=host,
            port=port,
            db=db_name,
            unix_socket=unix_socket,
            ssl=ssl,
            username=username,
            password=password,
            **kwargs
        )
        self._measurement_mapping = {}
        self.loop = loop or asyncio.get_event_loop()
        self._candles_buffer = asyncio.Queue(maxsize=1000, loop=self.loop)

    async def start(self):
        self._store_candles_task = self.loop.create_task(self.store_candles_coro())

    async def store_candle(self, trading_pair: TradingPair, candle: Candle):
        """Store candle in influx measurement.

        :param measurement: measurement name
        :param candle: Candle instance
        :return:
        """
        measurement = await self.get_measurement(trading_pair)
        influx_record = {
            "measurement": measurement,
            "time": candle.timestamp,
            "tags": {
                "interval": candle.interval,
                "version": 0
            },
            "fields": {
                'open': float(candle.open),
                'high': float(candle.high),
                'low': float(candle.low),
                'close': float(candle.close),
                'base_volume': to_float(candle.base_volume),
                'quote_volume': to_float(candle.quote_volume),
            }
        }
        try:
            self._candles_buffer.put_nowait(influx_record)
            logger.debug("Candle added to buffer")
        except asyncio.QueueFull:
            logger.warning("Candles buffer is full, can't store candle")

    async def store_candles_coro(self):
        """Store candles from buffer queue to influxdb.
        """
        while True:
            try:
                influx_record = await self._candles_buffer.get()
                logger.debug("Write candle: %s",
                             json.dumps(influx_record, indent=4))  # TODO: lazy dumps
                await self.client.write(influx_record)
                logger.debug('Written to influx')
                self._candles_buffer.task_done()
            except asyncio.CancelledError:
                logger.debug("store_candles_coro cancelled")
                break
            except ClientError:
                logger.exception("Exception happen while writting to InfluxDB."
                                 "Sleep 1 sec and retry.")
                await asyncio.sleep(1)
            except Exception:  # noqa
                logger.exception("Unhandled exception in store_candles_coro")
                break
        logger.info("Store candle coro stopped.")

    async def load_measurements_map(self):
        """Load measurements mapping from special measurement.

        `{exchange}:{symbol}` key is used to identify trading pair in measurement
        name mapping.
        """
        resp = await self.client.query(f"SELECT * FROM {MAPPING_MEASUREMENT};")
        if 'series' not in resp['results'][0]:
            # empty results, fresh mapping
            self._measurements_loaded = True
            return
        series = resp['results'][0]['series'][0]
        result = [dict(zip(series['columns'], row)) for row in series['values']]
        for item in result:
            key = f"{item['exchange']}:{item['symbol']}"
            self._measurement_mapping[key] = item['measurement']
        self._measurements_loaded = True

    async def get_measurement(self, trading_pair: TradingPair):
        """Get current trading pair measurement.

        If there is no measurement name for provided trading pair, then it will
        be generated and stored.

        :param trading_pair:
        :return:
        """
        assert self._measurements_loaded, \
            "You must load measurement mapping first using " \
            "`load_measurement_map` method "
        measurement = self._measurement_mapping.get(self._trading_pair_key(trading_pair))
        if not measurement:
            measurement = f"{trading_pair.exchange}_{trading_pair.symbol}_60"  # TODO: random suffix
            await self.update_measurement_name(trading_pair, measurement)
        return measurement

    async def update_measurement_name(self, trading_pair, measurement):
        await self.client.write({
            "measurement": MAPPING_MEASUREMENT,
            "fields": {
                "exchange": trading_pair.exchange,
                "symbol": trading_pair.symbol,
                "measurement": measurement,
            }
        })
        trading_pair.measurement = measurement
        self._measurement_mapping[self._trading_pair_key(trading_pair)] = measurement

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        self._store_candles_task.cancel()
        await self.client.close()

    def _trading_pair_key(self, trading_pair):
        return f"{trading_pair.exchange}:{trading_pair.symbol}"


class SeriesStorageSet:
    """Series storage set.

    Combine multiple series storage instances in a single one.
    """
    storage_instances: List[SeriesStorage]

    def __init__(self, storage_instances):
        self.storage_instances = storage_instances

    async def load_measurements_map(self):
        await self._gather_childs('load_measurements_map')

    async def store_candle(self, trading_pair: TradingPair, candle: Candle):
        await self._gather_childs(
            'store_candle', trading_pair=trading_pair, candle=candle
        )

    async def start(self):
        await self._gather_childs('start')

    async def close(self):
        await self._gather_childs('close')

    async def _gather_childs(self, method_name, *args, **kwargs):
        """Invoke series instances in parallel.

        :param method_name: method to invoke
        :param args: passed args
        :param kwargs: passed kwargs
        :return:
        """
        result = await asyncio.gather(
            *[getattr(s, method_name)(*args, **kwargs)
              for s in self.storage_instances],
            return_exceptions=True
        )
        for item in result:
            if isinstance(item, Exception):
                try:
                    raise item
                except Exception:  # noqa
                    logger.exception("Exception with one of storage")
