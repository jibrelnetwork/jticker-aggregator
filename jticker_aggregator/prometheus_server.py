import prometheus_client
from aiohttp import web
from mode import Service
from loguru import logger
from addict import Dict

from jticker_core import inject, register


@register(singleton=True)
def web_app():
    return web.Application()


@register(singleton=True, name="prometheus_server")
class PrometheusMetricsServer(Service):

    @inject
    def __init__(self, web_app: web.Application, config: Dict):
        super().__init__()
        self.app = web_app
        self.app.router.add_route("GET", "/metrics", self._metrics)
        self.host = config.prometheus_web_host
        self.port = int(config.prometheus_web_port)
        self.runner = None
        self.site = None

    async def on_start(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        logger.info("Serving on {}:{}", self.host, self.port)

    async def on_stop(self):
        if self.runner is not None:
            await self.runner.cleanup()

    async def _metrics(self, request):
        body = prometheus_client.exposition.generate_latest().decode("utf-8")
        content_type = prometheus_client.exposition.CONTENT_TYPE_LATEST
        return web.Response(body=body, headers={"Content-Type": content_type})
