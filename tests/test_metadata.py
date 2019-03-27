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
            }
        ]
    }

    example_create_response = {
        'id': 2,
        'exchange': {
            "id": 1,
            "name": "Binance",
            "slug": "binance"
        },
        'symbol': 'BTCUSD_NOT_EXIST',
        'base_asset': None,
        'quote_asset': None,
    }

    with mock.patch('jticker_aggregator.metadata.Metadata._get',
                    return_value=example_pair_list_response):
        with mock.patch('jticker_aggregator.metadata.Metadata._post',
                        return_value=example_create_response):
            yield Metadata()


@pytest.mark.asyncio
async def test_get_trading_pair():
    with metadata_mock() as metadata:
        pair = await metadata.get_trading_pair('binance', 'BTCUSD')
        repr(pair)


@pytest.mark.asyncio
async def test_get_new_trading_pair():
    with metadata_mock() as metadata:
        await metadata.get_trading_pair('binance', 'BTCUSD_NOT_EXIST')
