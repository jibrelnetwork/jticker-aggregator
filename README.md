# Jticker aggregator application

## Configuration

Available environment variables:

* `KAFKA_BOOTSTRAP_SERVERS` (default: `kafka:9092`) — comma separated list of kafka bootstrap servers
* `KAFKA_CANDLES_STUCK_TIMEOUT` (default: `600`) - fall down on stuck timeout
* `INFLUX_HOST` (default: `influxdb`) — comma-separated influxdb host list
* `INFLUX_PORT` (default: `8086`) — influxdb port
* `INFLUX_DB` (default: `test`) — influxdb database name **TODO: change default to `jticker`**
* `INFLUX_SSL` (default: `false`) — force use SSL (`true|false`)
* `INFLUX_USERNAME` (default: `None`) — influxdb user
* `INFLUX_PASSWORD` (default: `None`) — influxdb password
* `INFLUX_UNIX_SOCKET` (default: `None`) — unix socket to use
* `LOG_LEVEL` (default: `INFO`) — log level (`ERROR`, `INFO`, `WARN`, `DEBUG`)
* `SENTRY_DSN` (default: _empty_) — Sentry DSN connection string

## Notes
There is an issue: starting kafka and client at the same time leads to total freeze of clients. Start kafka first, when "started" message appears you can start aggregator/grabber.
