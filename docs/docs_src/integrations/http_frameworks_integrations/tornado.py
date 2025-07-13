import asyncio

import tornado.web

from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")


@broker.subscriber("test")
async def base_handler(body):
    print(body)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")


def make_app():
    return tornado.web.Application(
        [
            (r"/", MainHandler),
        ]
    )


async def main():
    app = make_app()
    app.listen(8888)

    await broker.start()
    try:
        await asyncio.Event().wait()
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
