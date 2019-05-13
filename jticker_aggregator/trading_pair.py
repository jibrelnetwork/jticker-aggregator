import logging
from typing import Optional


logger = logging.getLogger(__name__)


class TradingPair:

    """Trading pair (aggregator).
    """

    id: int
    exchange: str
    symbol: str
    base_asset: int
    quote_asset: int
    topic: Optional[str]

    def __init__(self, exchange, symbol, base_asset=None, quote_asset=None, topic=None):
        """Trading pair CTOR.

        :param exchange: slug of exchange where the trading pair is being traded
        :param symbol: trading pair symbol in exchange presentation
        :param base_asset: base asset (which price is measured by quote)
        :param quote_asset: quote asset (trading pair quote unit)
        """
        self.exchange = exchange
        self.symbol = symbol
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.topic = topic

    def __repr__(self):
        return f"<TradingPair {self.exchange}:{self.symbol}>"
