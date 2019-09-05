from mode import Service
from loguru import logger

from .injector import injector


@injector.register(name="aggregator")
class Aggregator(Service):

    @injector.inject
    def __init__(self, candle_provider):  # series_storages
        super().__init__()
        self.candle_provider = candle_provider

    def on_init_dependencies(self):
        return [
            self.candle_provider,
        ]
