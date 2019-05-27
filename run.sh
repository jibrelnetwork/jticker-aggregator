#!/usr/bin/env sh
RUNMODE="${1:-app}"

echo "Starting Jticker aggregator. Version: `cat /app/version.txt`; node: `hostname`"

# collect list of kafka nodes and form `-wait` arguemnts line for dockerize
KAFKA_LIST=`echo ${KAFKA_BOOTSTRAP_SERVERS} | tr "," "\n"`
KAFKA_WAIT=""
for x in ${KAFKA_LIST}
do
    KAFKA_WAIT="${KAFKA_WAIT} -wait tcp://${x}"
done

# collect list of influxdb instances and form `-wait` arguments line for dockerize
INFLUX_HOST_LIST=`echo ${INFLUX_HOST} | tr "," "\n"`
INFLUX_WAIT=""
for x in ${INFLUX_HOST_LIST}
do
    INFLUX_WAIT="${INFLUX_WAIT} -wait tcp://${x}:${INFLUX_PORT:-8086}"
done

dockerize ${INFLUX_WAIT} ${KAFKA_WAIT}

if [ "${RUNMODE}" = "app" ]; then
    python -m jticker_aggregator
elif [ "${RUNMODE}" = "test" ]; then
    pytest "${@:5}"
elif [ "${RUNMODE}" = "ptw" ]; then
    ptw "${@:4}"
else
    python manage.py "$@"
fi
