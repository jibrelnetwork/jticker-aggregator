from typing import Optional

from .trading_pair import TradingPair


class Candle:

    """Candle.

    Received from kafka and stored in series storage. Intermediate structure
    between kafka and influx db.
    """

    __slots__ = [
        'exchange',
        'symbol',
        'interval',
        'timestamp',
        'open',
        'high',
        'low',
        'close',
        'base_volume',
        'quote_volume',
        'version',
    ]

    #: exchange slug
    exchange: str
    #: trading pair symbol
    symbol: Optional[str]
    #: candle interval in seconds
    interval: int
    #: candle close time (iso datetime string)
    timestamp: str

    #: open price
    open: float
    #: high price
    high: float
    #: low price
    low: float
    #: close price
    close: float

    #: volume in base asset
    base_volume: Optional[float]
    #: volume in quote asset
    quote_volume: Optional[float]
    #: series version
    version: int

    def __init__(self, exchange: str, symbol: str,
                 interval: int, timestamp: str,
                 open: float, high: float, low: float, close: float,
                 base_volume=None, quote_volume=None,
                 version=1):
        self.exchange = exchange
        self.symbol = symbol
        self.interval = interval
        self.timestamp = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.base_volume = base_volume
        self.quote_volume = quote_volume
        self.version = version

    @property
    def trading_pair(self):
        return TradingPair.get(self.exchange, self.symbol)
