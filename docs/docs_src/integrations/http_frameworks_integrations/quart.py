from quart import Quart

from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")

app = Quart(__name__)


@broker.subscriber("test")
async def base_handler(body):
    print(body)


@app.before_serving
async def start_broker():
    await broker.start()


@app.after_serving
async def stop_broker():
    await broker.stop()


@app.route("/")
async def json():
    return {"hello": "world"}
