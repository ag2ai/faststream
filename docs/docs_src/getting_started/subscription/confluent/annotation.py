from faststream import FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("test-topic")
async def handle(
    name: str,
    user_id: int,
):
    assert name == "John"
    assert user_id == 1
