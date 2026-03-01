from typing import Annotated
from faststream import Context, FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

@broker.subscriber("test-queue")
async def handle(
    not_existed: Annotated[None, Context("not_existed", default=None)],
):
    assert not_existed is None
