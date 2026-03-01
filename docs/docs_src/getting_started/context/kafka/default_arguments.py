from typing import Annotated
from faststream import Context, FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

@broker.subscriber("test-topic")
async def handle(
    not_existed: Annotated[None, Context("not_existed", default=None)],
):
    assert not_existed is None
