from typing import Annotated
from faststream import Context
from faststream.mqtt import MQTTBroker

broker = MQTTBroker()

@broker.subscriber("test-topic")
async def handle(
    msg: str,
    collector: Annotated[list[str], Context(initial=list)],
):
    collector.append(msg)
