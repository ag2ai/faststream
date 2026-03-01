from faststream import FastStream
from faststream.exceptions import AckMessage
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(body):
    smth_processing(body)


def smth_processing(body):
    if True:
        raise AckMessage()


@app.after_startup
async def test_publishing():
    await broker.publish("Hello!", "test-queue")
