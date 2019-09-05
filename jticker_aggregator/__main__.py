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

# TODO: maybe move to jticker-core if injector is a good solution
from .injector import injector
from .aggregator import Aggregator
from .prometheus_server import PrometheusMetricsServer


@injector.register(singleton=True)
def version():
    try:
        return Path(__file__).parent.parent.joinpath("version.txt").read_text()
    except Exception:
        return "dev"


@injector.register(singleton=True)
@injector.inject
def config(version: str) -> Dict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sentry-dsn", default=None, help="Sentry DSN [default: %(default)s]")
    parser.add_argument("--kafka-bootstrap-servers", default="kafka:9092",
                        help="Comma separated kafka bootstrap servers [default: %(default)s]")
    parser.add_argument("--log-level", default="INFO",
                        help="Python logging level [default: %(default)s]")
    # host = socket.gethostname()
    # parser.add_argument("--http-user-agent", default=f"jticker-aggregator/{version} {host}",
    #                     help="")
    parser.add_argument("--influx-host", default="influxdb",
                        help="Influxdb host [default: %(default)s]")
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
    parser.add_argument("--prometheus-web-host", default="0.0.0.0",
                        help="Prometheus web server host [default: %(default)s]")
    parser.add_argument("--prometheus-web-port", default="8080",
                        help="Prometheus web server port [default: %(default)s]")
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


@injector.inject
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


# import logging
# import asyncio

# import sentry_sdk

# from .consumer import Consumer

# from .series import SeriesStorage, SeriesStorageSet
# from .logging import _configure_logging
# from .stats import AggregatorStats

# from .settings import (
#     SENTRY_DSN,
#     KAFKA_BOOTSTRAP_SERVERS,
#     INFLUX_HOST,
#     INFLUX_PORT,
#     INFLUX_USERNAME,
#     INFLUX_PASSWORD,
#     INFLUX_UNIX_SOCKET,
#     INFLUX_SSL,
#     INFLUX_DB,
#     LOG_LEVEL,
# )


# logger = logging.getLogger('jticker_aggregator')

# loop = asyncio.get_event_loop()


# if SENTRY_DSN:
#     with open('version.txt', 'r') as fp:
#         sentry_sdk.init(SENTRY_DSN, release=fp.read().strip())


# async def consume():
#     _configure_logging(LOG_LEVEL)

#     stats = AggregatorStats()
#     await stats.start()

#     influx_instances = []

#     for host in INFLUX_HOST.split(','):
#         influx_instances.append(SeriesStorage(
#             host=host,
#             db_name=INFLUX_DB,
#             port=INFLUX_PORT,
#             ssl=INFLUX_SSL,
#             unix_socket=INFLUX_UNIX_SOCKET,
#             username=INFLUX_USERNAME,
#             password=INFLUX_PASSWORD,
#             loop=loop,
#             stats=stats,
#         ))

#     storage = SeriesStorageSet(influx_instances)

#     await storage.load_measurements_map()
#     await storage.start()

#     consumer = Consumer(
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#         group_id="aggregator",
#         loop=loop,
#     )

#     try:
#         await storage.load_measurements_map()

#         await consumer.start()

#         async for candle in consumer:
#             await storage.store_candle(candle)
#     except:  # noqa
#         sentry_sdk.capture_exception()
#         logger.exception("Exit on unhandled exception")
#     finally:
#         logger.info("Stopping consumer (finally)")
#         # Will leave consumer group; perform autocommit if enabled.
#         await consumer.stop()


# loop.run_until_complete(consume())
