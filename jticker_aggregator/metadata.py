import logging
from typing import Dict, Optional
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
    measurement: Optional[str]
    topic: Optional[str]

    def __init__(self, id, exchange, symbol, base_asset, quote_asset,
                 measurement=None, topic=None):
        """Trading pair CTOR.

        :param id: internal trading pair id
        :param exchange: slug of exchange where the trading pair is being traded
        :param symbol: trading pair symbol in exchange presentation
        :param base_asset: base asset (which price is measured by quote)
        :param quote_asset: quote asset (trading pair quote unit)
        :param measurement: actual influxdb measurement name
        :param topic: actual kafka topic name
        """
        self.id = id
        self.exchange = exchange
        self.symbol = symbol
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.measurement = measurement
        if self.measurement is None:
            self.gen_measurement_name()
        self.topic = topic

    def gen_measurement_name(self):
        """Generate influx measurement name for trading pair.

        Can be used if measurement didn't provided to constructor.
        """
        assert self.id, "Can't generate measurement name without id"
        assert self.measurement is None, "Measurement already defined"
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

    #: Flag informing that trading pairs loaded into memory and can be queried
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

        TODO: add assets arguments for cases when we know somehow the assets
            related to this trading pair

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
        resp_data = await self._post(url, json=data)
        trading_pair = self._load_pair(resp_data)
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
        url = urljoin(
            self.service_url,
            f'/v1/trading_pairs/{trading_pair.id}/set_measurement'
        )
        response = await self._post(url, data=measurement.encode())
        logger.debug('Measurement for trading pair %i updated %s:\n%s',
                     trading_pair.id, measurement, response)

    async def _load_trading_pairs(self):
        """Load trading pairs into memory.

        :return:
        """
        url = urljoin(self.service_url, '/v1/trading_pairs/')
        data = await self._get(url)

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

    async def _get(self, url):  # pragma: no cover
        async with ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.error('Error while loading trading pairs (%i): %s',
                                 resp.status, await resp.text())
                    raise Exception("Request failed GET %s", url)
                else:
                    data = await resp.json()
        return data

    async def _post(self, url, **kwargs):  # pragma: no cover
        async with ClientSession() as session:
            async with session.post(url, **kwargs) as resp:
                if not resp.status == 200:
                    logger.error(
                        "Cant create symbol because of metadata service error:"
                        "\n%s", await resp.text()
                    )
                    raise Exception("Request failed POST %s %s", url, kwargs)
                else:
                    data = await resp.json()
        return data
