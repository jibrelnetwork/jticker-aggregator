import logging
from typing import Dict, Optional

from aioinflux import InfluxDBClient

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

    def __init__(self,
                 host: str = "localhost",
                 port: int = 8086,
                 db_name: str = "jticker",
                 ssl: bool = False,
                 unix_socket: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
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

    async def store_candle(self, measurement: str, candle: Candle):
        """Store candle in influx measurement.

        :param measurement: measurement name
        :param candle: Candle instance
        :return:
        """
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
        logger.debug("Write candle %s to influx (%s measurement): %s",
                     candle, measurement, influx_record)
        await self.client.write(influx_record)

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

    async def get_measurement(self, trading_pair):
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
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.client.close()

    def _trading_pair_key(self, trading_pair):
        return f"{trading_pair.exchange}:{trading_pair.symbol}"
