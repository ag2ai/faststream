from sanic import Sanic
from sanic.response import text

from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")

app = Sanic("MyHelloWorldApp")


@broker.subscriber("test")
async def base_handler(body):
    print(body)


@app.after_server_start
async def start_broker(app, loop):
    await broker.start()


@app.after_server_stop
async def stop_broker(app, loop):
    await broker.stop()


@app.get("/")
async def hello_world(request):
    return text("Hello, world.")
