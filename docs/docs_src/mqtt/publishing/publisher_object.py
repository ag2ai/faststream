from faststream import FastStream
from faststream.mqtt import MQTTBroker, QoS

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)

events = broker.publisher(
    "devices/events",
    qos=QoS.AT_LEAST_ONCE,
    headers={"source": "sensor"},
)


@broker.subscriber("devices/commands")
async def handle_command(command: str) -> None:
    await events.publish({"command": command}, headers={"kind": "echo"})


@broker.subscriber("devices/events")
async def handle_event(event: dict) -> None:
    print(event)
