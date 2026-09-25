from typing import Annotated
from faststream import Context, FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)

@broker.subscriber("test-topic")
async def handle(
    not_existed: Annotated[None, Context("not_existed", default=None)],
):
    assert not_existed is None
