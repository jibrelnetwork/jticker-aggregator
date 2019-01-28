import logging
from typing import Optional

from aioinflux import InfluxDBClient

from .candle import Candle


logger = logging.getLogger(__name__)


def to_float(v):
    return float(v) if v is not None else None


class SeriesStorage:

    """Candle series storage abstraction for aggregator.

    Can be used as context manager:

        async with SeriesStorage() as storage:
            pass

    or you should close session explicitly:

        try:
            storage = SeriesStorage()
        finally:
            storage.close()
    """

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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.client.close()
