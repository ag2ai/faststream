from faststream import FastStream
from faststream.mq import MQBroker

broker = MQBroker(queue_manager="QM1")
app = FastStream(broker)


@broker.subscriber("input")
@broker.publisher("output")
async def handle(msg: str) -> str:
    return msg.upper()


@app.after_startup
async def test_publishing() -> None:
    await broker.publish("hello", "input")
