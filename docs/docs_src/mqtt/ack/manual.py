from faststream import AckPolicy, FastStream
from faststream.mqtt import MQTTBroker, MQTTMessage, QoS

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.subscriber("jobs/run", qos=QoS.AT_LEAST_ONCE, ack_policy=AckPolicy.MANUAL)
async def work(payload: dict, msg: MQTTMessage) -> None:
    try:
        print(payload)
    finally:
        await msg.ack()
