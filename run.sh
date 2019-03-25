#!/usr/bin/env sh
RUNMODE="${1:-app}"

echo "Starting Jticker aggregator. Version: `cat /app/version.txt`; node: `hostname`"

if [ "${RUNMODE}" = "app" ]; then
    python -m jticker_aggregator
elif [ "${RUNMODE}" = "test" ]; then
    pytest "${@:5}"
elif [ "${RUNMODE}" = "ptw" ]; then
    ptw "${@:4}"
else
    python manage.py "$@"
fi
