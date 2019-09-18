import os
import asyncio
import argparse
import collections
from pathlib import Path

import sentry_sdk
import mode
import prometheus_client
from loguru import logger
from addict import Dict
from aiohttp import web

from jticker_core import configure_logging, ignore_aiohttp_ssl_eror, inject, register, WebServer

from .aggregator import Aggregator


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
    parser.add_argument("--trading-pair-queue-timeout", default="1",
                        help="Internal trading pair queue establish timeout [default: %(default)s]")
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
    parser.add_argument("--web-host", default="0.0.0.0",
                        help="Prometheus web server host [default: %(default)s]")
    parser.add_argument("--web-port", default="8080",
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


async def healthcheck(request):
    return web.json_response(dict(healthy=True))


async def metrics(request):
    body = prometheus_client.exposition.generate_latest().decode("utf-8")
    content_type = prometheus_client.exposition.CONTENT_TYPE_LATEST
    return web.Response(body=body, headers={"Content-Type": content_type})


@register
@inject
def host(config):
    return config.web_host


@register
@inject
def port(config):
    return config.web_port


@inject
def main(config: Dict, version: str, aggregator: Aggregator, web_server: WebServer):
    web_server.app.router.add_route("GET", "/healthcheck", healthcheck)
    web_server.app.router.add_route("GET", "/metrics", metrics)

    loop = asyncio.get_event_loop()
    ignore_aiohttp_ssl_eror(loop, "3.5.4")
    configure_logging(config.log_level)
    if config.sentry_dsn:
        sentry_sdk.init(config.sentry_dsn, release=version)
    logger.info("Jticker aggregator version {}", version)
    Worker(aggregator, web_server).execute_from_commandline()


main()
