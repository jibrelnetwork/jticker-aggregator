from contextlib import contextmanager

import pytest
from asynctest import mock

from jticker_aggregator.metadata import Metadata


@contextmanager
def metadata_mock():
    example_pair_list_response = {
        'page': 1,
        'total': 1,
        'result': [
            {
                "id": 1,
                "symbol": "QTUMBNB",
                "exchange": {
                    "id": 1,
                    "name": "Binance",
                    "slug": "binance"
                },
                "base_asset": None,
                "quote_asset": None,
            },
            {
                "id": 2,
                "symbol": "BTCUSD",
                "exchange": {
                    "id": 1,
                    "name": "Binance",
                    "slug": "binance"
                },
                "base_asset": {
                    "symbol": "BTC"
                },
                "quote_asset": {
                    "symbol": "USD"
                }
            }
        ]
    }

    with mock.patch('jticker_aggregator.metadata.Metadata._get',
                    return_value=example_pair_list_response):
        yield Metadata()


@pytest.mark.asyncio
async def test_get_trading_pair():
    with metadata_mock() as metadata:
        pair = await metadata.get_trading_pair('binance', 'BTCUSD')
        repr(pair)
