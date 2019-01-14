from contextlib import contextmanager

import pytest
from asynctest import mock

from jticker_aggregator.metadata import Metadata


@contextmanager
def metadata_mock():
    example_pair_list_response = [
        {
            'id': 1,
            'exchange': 'binance',
            'symbol': 'BTCUSD',
            'base_asset': None,
            'quote_asset': None
        }
    ]

    example_create_response = {
        'id': 1,
        'exchange': 'binance',
        'symbol': 'SOMENEW',
        'base_asset': None,
        'quote_asset': None
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
        await metadata.get_trading_pair('binance', 'BTCUSD_NOT_EXIST')


@pytest.mark.asyncio
async def test_update_measurement():
    with metadata_mock() as metadata:
        pair = await metadata.get_trading_pair('binance', 'BTCUSD')

        await metadata.update_measurement(pair)


@pytest.mark.asyncio
async def test_sync_trading_pair():
    with metadata_mock() as metadata:
        await metadata.sync_trading_pair('binance', 'BTCUSD_NOT_EXIST')
