from faststream import FastStream, AckPolicy
from faststream.exceptions import AckMessage
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber(
    "test-error-topic", group_id="test-error-group", ack_policy=AckPolicy.REJECT_ON_ERROR, auto_offset_reset="earliest"
)
async def handle(body):
    smth_processing(body)


def smth_processing(body):
    if True:
        raise AckMessage()


@app.after_startup
async def test_publishing():
    await broker.publish("Hello!", "test-error-topic")
