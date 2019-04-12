# Jticker aggregator application

## Configuration

Available environment variables:

* `KAFKA_BOOTSTRAP_SERVERS` (default: `kafka:9092`) — comma separated list of kafka bootstrap servers
* `INFLUX_HOST` (default: `influxdb`) — influxdb host
* `INFLUX_PORT` (default: `8086`) — influxdb port
* `INFLUX_DB` (default: `test`) — influxdb database name **TODO: change default to `jticker`**
* `INFLUX_SSL` (default: `false`) — force use SSL (`true|false`)
* `INFLUX_USERNAME` (default: `None`) — influxdb user
* `INFLUX_PASSWORD` (default: `None`) — influxdb password
* `INFLUX_UNIX_SOCKET` (default: `None`) — unix socket to use
* `METADATA_URL` (default: `http://meta:8000`) — `meta` service url accessible from aggregator instance
* `LOG_LEVEL` (default: `INFO`) — log level (`ERROR`, `INFO`, `WARN`, `DEBUG`)
* `SENTRY_DSN` (default: _empty_) — Sentry DSN connection string
