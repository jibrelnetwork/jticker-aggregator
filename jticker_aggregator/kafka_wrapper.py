from functools import partial
from threading import Thread
from asyncio import Lock

import janus
from kafka import KafkaConsumer


class Consumer:

    def __init__(self, *topics, poll_time_ms=50, **config):
        self._thread = Thread(target=self._run)
        self._consumer = KafkaConsumer(*topics, **config)
        self._poll_time_ms = poll_time_ms
        self._command = janus.Queue(maxsize=1)
        self._command_lock = Lock()
        self._response = janus.Queue(maxsize=1)
        self._consumed = janus.Queue(maxsize=1000)

    async def start(self):
        self._thread.start()

    async def __aenter__(self):
        await self.start()
        return self

    async def stop(self):
        await self._call(None)
        self._thread.join()

    async def __aexit__(self, *exc_info):
        await self.stop()

    async def _call(self, method, *args, **kwargs):
        async with self._command_lock:
            await self._command.async_q.put((method, args, kwargs))
            return await self._response.async_q.get()

    def __getattr__(self, name):
        return partial(self._call, name)

    async def __aiter__(self):
        while True:
            yield await self._consumed.async_q.get()

    def _run(self):
        while True:
            while not self._command.sync_q.empty():
                method, args, kwargs = self._command.sync_q.get()
                if method is None:
                    break
                result = getattr(self._consumer, method)(*args, **kwargs)
                self._response.sync_q.put(result)
            result = self._consumer.poll(timeout_ms=self._poll_time_ms)
            for topic, records in result.items():
                for record in records:
                    self._consumed.sync_q.put(record)
