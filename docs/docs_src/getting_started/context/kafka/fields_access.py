from typing import Annotated
from faststream import Context, FastStream
from faststream.kafka import KafkaBroker, KafkaMessage

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

@broker.subscriber("test-topic")
async def handle(
    msg: KafkaMessage,
    correlation_id: Annotated[str, Context("message.correlation_id")],
    user_header: Annotated[str, Context("message.headers.user")],
):
    assert msg.correlation_id == correlation_id
    assert msg.headers["user"] == user_header
