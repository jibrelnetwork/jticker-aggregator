import os
import asyncio
import argparse
import collections
from pathlib import Path

import sentry_sdk
import mode
from loguru import logger
from addict import Dict

from jticker_core import configure_logging, ignore_aiohttp_ssl_eror

from .injector import inject, register
from .aggregator import Aggregator
from .prometheus_server import PrometheusMetricsServer


@register(singleton=True)
def version():
    try:
        return Path(__file__).parent.parent.joinpath("version.txt").read_text()
    except Exception:
        return "dev"


@register(singleton=True)
@inject
def config(version: str) -> Dict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sentry-dsn", default=None, help="Sentry DSN [default: %(default)s]")
    parser.add_argument("--log-level", default="INFO",
                        help="Python logging level [default: %(default)s]")
    parser.add_argument("--stats-log-interval", default="60",
                        help="Stats logging interval [default: %(default)s]")
    # influx
    parser.add_argument("--influx-host", default="influxdb",
                        help="Influxdb hosts (comma separated) [default: %(default)s]")
    parser.add_argument("--influx-port", default="8086",
                        help="Influxdb port [default: %(default)s]")
    parser.add_argument("--influx-db", default="test",
                        help="Influxdb db [default: %(default)s]")
    parser.add_argument("--influx-username", default=None, help="Influxdb username [%(default)s]")
    parser.add_argument("--influx-password", default=None, help="Influxdb password [%(default)s]")
    parser.add_argument("--influx-ssl", action="store_true", default=False,
                        help="Influxdb use ssl [%(default)s]")
    parser.add_argument("--influx-unix-socket", default=None,
                        help="Influxdb unix socket [%(default)s]")
    parser.add_argument("--influx-measurements-mapping", default="mapping",
                        help="Influxdb unix socket [%(default)s]")
    # web
    parser.add_argument("--prometheus-web-host", default="0.0.0.0",
                        help="Prometheus web server host [default: %(default)s]")
    parser.add_argument("--prometheus-web-port", default="8080",
                        help="Prometheus web server port [default: %(default)s]")
    # kafka
    parser.add_argument("--kafka-bootstrap-servers", default="kafka:9092",
                        help="Comma separated kafka bootstrap servers [default: %(default)s]")
    parser.add_argument("--kafka-trading-pairs-topic", default="assets_metadata",
                        help="Trading pairs kafka topic [default: %(default)s]")
    parser.add_argument("--kafka-candles-consumer-group-id", default="aggregator",
                        help="Kafka candles consumer group id [default: %(default)s]")
    args = vars(parser.parse_args())
    env = {k.lower(): v for k, v in os.environ.items() if k.lower() in args}
    return Dict(**collections.ChainMap(env, args))


class Worker(mode.Worker):
    """Patched `Worker` with disabled `_setup_logging` and graceful shutdown.
    Default `mode.Worker` overrides logging configuration. This hack is there to
    deny this behavior.
    """

    def _setup_logging(self) -> None:
        pass


@inject
def main(config: Dict, version: str, aggregator: Aggregator,
         prometheus_server: PrometheusMetricsServer):
    loop = asyncio.get_event_loop()
    ignore_aiohttp_ssl_eror(loop, "3.5.4")
    configure_logging(config.log_level)
    if config.log_level == "DEBUG":
        loop.set_debug(True)
    if config.sentry_dsn:
        sentry_sdk.init(config.sentry_dsn, release=version)
    logger.info("Jticker aggregator version {}", version)
    Worker(aggregator, prometheus_server).execute_from_commandline()


main()
