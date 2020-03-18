import asyncio
from pathlib import Path

import sentry_sdk
from addict import Dict
from loguru import logger

from jticker_core import Worker, configure_logging, ignore_aiohttp_ssl_eror, inject, register

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
    # time series
    base_parser.add_argument("--time-series-chunk-size", default="1000",
                             help="Influx batch/chunk write size [%(default)s]")
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
