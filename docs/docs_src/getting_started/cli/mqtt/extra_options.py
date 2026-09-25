from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker()
app = FastStream(broker)


@broker.subscriber("my-topic")
async def handler(msg: dict) -> None:
    print(f"Received: {msg}")


@app.on_startup
async def startup(port: int, foo: str) -> None:
    print("Port:", port)
    print("Foo:", foo)
