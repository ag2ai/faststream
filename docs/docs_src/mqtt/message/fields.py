from faststream import FastStream
from faststream.mqtt import MQTTBroker
from faststream.mqtt.annotations import MQTTMessage

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.subscriber("devices/+/status")
async def handle(msg: MQTTMessage) -> None:
    props = msg.raw_message.properties  # MQTT 5.0 only
    print(props)
