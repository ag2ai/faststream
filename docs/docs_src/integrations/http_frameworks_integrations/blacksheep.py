from blacksheep import Application

from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")

app = Application()


@broker.subscriber("test")
async def base_handler(body):
    print(body)


@app.on_start
async def start_broker(application: Application) -> None:
    await broker.start()


@app.on_stop
async def stop_broker(application: Application) -> None:
    await broker.stop()


@app.route("/")
async def home():
    return "Hello, World!"
