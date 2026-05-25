from faststream import FastStream, Path
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.subscriber("/devices/{device_id}/temperature")
async def on_temperature(
    body: str,
    device_id: str = Path(),
) -> None:
    print(device_id, body)
