from faststream import FastStream
from faststream.mqtt import MQTTBroker, QoS
from faststream.mqtt.response import MQTTResponse

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.subscriber("rpc/status")
async def status() -> MQTTResponse:
    return MQTTResponse(
        {"ok": True},
        qos=QoS.AT_LEAST_ONCE,
        retain=True,
    )
