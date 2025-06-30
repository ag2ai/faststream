from faststream import FastStream, AckPolicy
from faststream.exceptions import AckMessage
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber(
    "test-topic", group_id="test-group", ack_policy=AckPolicy.REJECT_ON_ERROR,
)
async def handle(body):
    smth_processing(body)


def smth_processing(body):
    if True:
        raise AckMessage()


@app.after_startup
async def test_publishing():
    await broker.publish("Hello!", "test-topic")
