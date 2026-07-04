from typing import Annotated
from faststream import Context, FastStream
from faststream.mqtt import MQTTBroker
from faststream.mqtt.message import MQTTMessage

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)

@broker.subscriber("test")
async def base_handler(
    body: str,
    message: Annotated[MQTTMessage, Context()],  # get access to raw message
):
    ...
