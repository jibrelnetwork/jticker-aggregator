import asyncio
from pathlib import Path

import sentry_sdk
from loguru import logger
from addict import Dict

from jticker_core import configure_logging, ignore_aiohttp_ssl_eror, inject, register, Worker

from .aggregator import Aggregator


@register(singleton=True)
def version():
    try:
        return Path(__file__).parent.parent.joinpath("version.txt").read_text()
    except Exception:
        return "dev"


@register(singleton=True)
@inject
def parser(base_parser, version: str) -> Dict:
    base_parser.add_argument("--stats-log-interval", default="60",
                             help="Stats logging interval [default: %(default)s]")
    base_parser.add_argument("--trading-pair-queue-timeout", default="1",
                             help="Internal trading pair queue establish timeout "
                                  "[default: %(default)s]")
    # time series
    base_parser.add_argument("--time-series-chunk-size", default="10000",
                             help="Influx batch/chunk write size [%(default)s]")
    # kafka
    base_parser.add_argument("--kafka-bootstrap-servers", default="kafka:9092",
                             help="Comma separated kafka bootstrap servers [default: %(default)s]")
    base_parser.add_argument("--kafka-trading-pairs-topic", default="assets_metadata",
                             help="Trading pairs kafka topic [default: %(default)s]")
    base_parser.add_argument("--kafka-candles-consumer-group-id", default="aggregator",
                             help="Kafka candles consumer group id [default: %(default)s]")
    base_parser.add_argument("--kafka-candles-stuck-timeout", default="600",
                             help="Kafka candle consumer stuck timeout in seconds "
                                  "[default: %(default)s]")
    return base_parser


@inject
def main(config: Dict, version: str, aggregator: Aggregator):
    loop = asyncio.get_event_loop()
    ignore_aiohttp_ssl_eror(loop, "3.5.4")
    configure_logging()
    if config.sentry_dsn:
        sentry_sdk.init(config.sentry_dsn, release=version)
    logger.info("Jticker aggregator version {}", version)
    Worker(aggregator).execute_from_commandline()


main()
