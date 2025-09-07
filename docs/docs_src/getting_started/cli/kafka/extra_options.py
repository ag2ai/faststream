from faststream import FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker()
app = FastStream(broker)


@broker.subscriber("my-subject")
async def handler(msg: dict) -> None:
    print(f"Received: {msg}")


@app.on_startup
async def startup(port: int, foo: str) -> None:
    print("Port:", port)
    print("Foo:", foo)
