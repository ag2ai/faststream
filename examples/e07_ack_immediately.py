from faststream import FastStream
from faststream.exceptions import AckMessage
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(body: str) -> None:
    smth_processing(body)


def smth_processing(body: str) -> None:
    if True:
        # interrupt msg processing and ack it
        raise AckMessage()


@app.after_startup
async def test_publishing() -> None:
    await broker.publish("Hello!", "test-queue")
