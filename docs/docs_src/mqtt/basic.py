from faststream import FastStream
from faststream.mqtt import MQTTBroker, MQTTMessage, QoS

broker = MQTTBroker("localhost", port=1883, version="5.0")
app = FastStream(broker)


@broker.subscriber(
    "sensors/+/temp",
    qos=QoS.AT_LEAST_ONCE,
    # shared="workers",  # optional: $share/workers/... for load-balanced consumers
    # max_workers=4,     # optional: concurrent handler tasks
)
async def on_temp(degrees: float, message: MQTTMessage) -> None:
    print(message.raw_message.topic)


@app.after_startup
async def publish_demo() -> None:
    await broker.publish(21.5, "sensors/room1/temp", qos=QoS.AT_LEAST_ONCE)
