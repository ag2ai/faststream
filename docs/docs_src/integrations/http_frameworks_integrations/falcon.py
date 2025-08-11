import falcon
import falcon.asgi

from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")


@broker.subscriber("test")
async def base_handler(body):
    print(body)


class ThingsResource:
    async def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.content_type = falcon.MEDIA_TEXT
        resp.text = (
            "\nTwo things awe me most, the starry sky "
            "above me and the moral law within me.\n"
            "\n"
            "    ~ Immanuel Kant\n\n"
        )


class StreamMiddleware:
    async def process_startup(self, scope, event):
        await broker.start()

    async def process_shutdown(self, scope, event):
        await broker.stop()


app = falcon.asgi.App()
app.add_middleware(StreamMiddleware())
app.add_route("/things", ThingsResource())
