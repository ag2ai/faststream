from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream(broker)


@broker.subscriber("my-subject")
async def handler(msg: dict) -> None:
    print(f"Received: {msg}")


@app.on_startup
async def startup(worker_id: int) -> None:
    print(f"Worker {worker_id} started")
