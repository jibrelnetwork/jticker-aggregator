import logging
from typing import Dict
from collections import defaultdict

from urllib.parse import urljoin
from aiohttp import ClientSession


logger = logging.getLogger(__name__)


class TradingPair:

    """Trading pair (aggregator).
    """

    id: int
    exchange: str
    symbol: str
    base_asset: int
    quote_asset: int
    measurement: str

    def __init__(self, id, exchange, symbol, base_asset, quote_asset,
                 measurement=None):
        """
        TODO: strict interface

        :param kwargs:
        """
        self.id = id
        self.exchange = exchange
        self.symbol = symbol
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.measurement = measurement

    def gen_measurement_name(self):
        """Generate influx measurement name for trading pair.
        """
        assert self.id, "Can't generate measurement name without id"
        self.measurement = f'ticker_{self.id}'

    def __repr__(self):
        return f"<TradingPair {self.id}:{self.exchange}:{self.symbol}>"


class Metadata:

    """Metadata provider.

    Helps to abstract from meta-data service.
    """

    #: Map symbols to trading pairs by exchange
    _trading_pair_by_symbol: Dict[str, Dict[str, TradingPair]]
    _trading_pair_by_id: Dict[int, TradingPair]
    _trading_pairs_loaded = False

    def __init__(self, service_url="http://meta:8000/", api_version=1):
        self.service_url = service_url
        self.api_version = api_version

        self._trading_pair_by_symbol = defaultdict(dict)
        self._trading_pair_by_id = {}

    async def get_trading_pair(self, exchange: str, symbol: str):
        """Get TradingPair for provided symbol and exchange.

        :param symbol: exchange internal representation
        :param exchange:
        :return:
        """
        if not self._trading_pairs_loaded:
            await self._load_trading_pairs()

        if symbol in self._trading_pair_by_symbol[exchange]:
            return self._trading_pair_by_symbol[exchange][symbol]
        return await self.create_trading_pair(exchange, symbol)

    async def create_trading_pair(self, exchange, symbol, measurement=None):
        """Create and store trading pair in metadata service.

        TODO: add assets arguments

        :param exchange:
        :param symbol:
        :param measurement:
        :return:
        """
        url = urljoin(self.service_url, '/v1/trading_pairs/')
        data = {
            'exchange': exchange,
            'symbol': symbol,
        }
        if measurement:
            data['measurement'] = measurement

        async with ClientSession() as session:
            async with session.post(url, json=data) as resp:
                if not resp.status == 200:
                    logger.error(
                        "Cant create symbol because of metadata service error:"
                        "\n%s", await resp.text()
                    )
                else:
                    trading_pair = self._load_pair(await resp.json())
                    logger.info('New trading pair created %s', trading_pair)
                    return trading_pair

    async def sync_trading_pair(self, exchange, symbol):
        """Sync trading pair with metadata service.

        Create trading pair if it is not present in metadata, generate
        measurement name if it was empty.

        :param exchange:
        :param symbol:
        :return:
        """
        trading_pair = await self.get_trading_pair(exchange, symbol)
        if not trading_pair:
            raise Exception("Trading pair not found %s %s", exchange, symbol)
        if not trading_pair.measurement:
            trading_pair.gen_measurement_name()
            await self.update_measurement(trading_pair)
        return trading_pair

    async def update_measurement(self, trading_pair: TradingPair):
        """Set active influxdb measurement for trading pair.

        :param trading_pair_id:
        :param kafka_topic:
        :return:
        """
        assert trading_pair.measurement, "Can't update measurement to None"
        measurement = trading_pair.measurement
        async with ClientSession() as session:
            url = urljoin(
                self.service_url,
                f'/v1/trading_pairs/{trading_pair.id}/set_measurement'
            )
            async with session.post(url, data=measurement.encode()) as resp:
                if resp.status == 200:
                    logger.debug('Measurement for trading pair %i updated %s',
                                 trading_pair.id, measurement)
                else:
                    logger.error(
                        'Measurement not updated because of error (%i):\n %s',
                        resp.status, await resp.text()
                    )

    async def _load_trading_pairs(self):
        """Load trading pairs into memory.

        :return:
        """
        async with ClientSession() as session:
            url = urljoin(self.service_url, '/v1/trading_pairs/')
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.error('Error while loading trading pairs (%i): %s',
                                 resp.status, await resp.text())
                data = await resp.json()
                for trading_pair_data in data:
                    self._load_pair(trading_pair_data)

    def _load_pair(self, trading_pair_data) -> TradingPair:
        """Load pair to memory.

        Index data for fast access in future.

        :param trading_pair_data:
        :return:
        """
        trading_pair = TradingPair(**trading_pair_data)
        self._trading_pair_by_symbol[trading_pair.exchange][trading_pair.symbol] = trading_pair  # noqa
        self._trading_pair_by_id[trading_pair.id] = trading_pair
        return trading_pair
