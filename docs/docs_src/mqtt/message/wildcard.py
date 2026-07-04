from faststream import FastStream
from faststream.mqtt import MQTTBroker
from faststream.mqtt.annotations import MQTTMessage

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.subscriber("/devices/+/logs/#")
async def on_logs(
    msg: MQTTMessage,
) -> None:
    print(msg.raw_message.topic)
