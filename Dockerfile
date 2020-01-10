FROM python:3.7-slim as builder

RUN apt update && apt install -y \
        build-essential libsnappy-dev
COPY jticker-core/requirements.txt /requirements-core.txt
COPY requirements.txt /
RUN pip install --no-cache-dir -r requirements-core.txt -r requirements.txt

FROM python:3.7-slim as runner

RUN apt update && apt install -y \
        libsnappy-dev
COPY --from=builder /usr/local/lib/python3.7/site-packages/ /usr/local/lib/python3.7/site-packages/
COPY . /app

WORKDIR /app
RUN pip install --no-cache-dir \
    -e ./jticker-core \
    -e ./

ENTRYPOINT ["python", "-m", "jticker_aggregator"]
