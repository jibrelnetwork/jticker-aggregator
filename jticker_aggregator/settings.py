import socket
from os import getenv
from os.path import dirname
from pathlib import Path

BASE_PATH = Path(dirname(__file__)) / '..'

try:
    with open(BASE_PATH / 'version.txt') as fp:
        VERSION = fp.read().strip()
except:
    # pylama:ignore=E722
    VERSION = 'dev'

SENTRY_DSN = getenv('SENTRY_DSN')

KAFKA_BOOTSTRAP_SERVERS = getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

INFLUX_HOST = getenv('INFLUX_HOST', 'influxdb')
INFLUX_PORT = int(getenv('INFLUX_PORT', '8086'))
INFLUX_DB = getenv('INFLUX_DB', 'test')
INFLUX_USERNAME = getenv('INFLUX_USERNAME')
INFLUX_PASSWORD = getenv('INFLUX_PASSWORD')
INFLUX_SSL = bool(getenv('INFLUX_SSL', 'false') == 'true')
INFLUX_UNIX_SOCKET = getenv('INFLUX_UNIX_SOCKET')

METADATA_URL = getenv('METADATA_URL', 'http://meta:8000/')

LOG_LEVEL = getenv('LOG_LEVEL', 'INFO')

HTTP_USER_AGENT = f'jticker-aggregator/{VERSION} {socket.gethostname()}'
