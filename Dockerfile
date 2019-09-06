FROM python:3.7-alpine

ENV KAFKA_BOOTSTRAP_SERVERS "kafka:9092"
ENV INFLUX_HOST "influxdb"
ENV INFLUX_PORT "8086"
ENV INFLUX_DB "test"
ENV INFLUX_SSL ""
ENV INFLUX_USERNAME ""
ENV INFLUX_PASSWORD ""
ENV INFLUX_UNIX_SOCKET ""
ENV LOG_LEVEL "INFO"
ENV SENTRY_DSN ""
ENV DOCKERIZE_VERSION "v0.6.1"

RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz

RUN addgroup -g 111 app \
 && adduser -D -u 111 -G app app \
 && mkdir -p /app \
 && chown -R app:app /app

RUN apk update

# build dependencies
RUN apk add gcc musl-dev g++

# optional aiokafka dependency https://aiokafka.readthedocs.io/en/stable/#optional-snappy-install
RUN apk add snappy-dev

WORKDIR /app

COPY --chown=app:app . /app/
RUN pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir -e ./jticker-core
RUN pip install --no-cache-dir -e .

USER app

ENTRYPOINT ["/app/run.sh", "app"]
