from faststream import FastStream
from faststream.mq import MQBroker

broker = MQBroker(queue_manager="QM1")
app = FastStream(broker)


@broker.subscriber("rpc.queue")
async def handle(msg: str) -> str:
    return f"processed:{msg}"


@app.after_startup
async def make_request() -> None:
    response = await broker.request("ping", "rpc.queue")
    assert await response.decode() == "processed:ping"
