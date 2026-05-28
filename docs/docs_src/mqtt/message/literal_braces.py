from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.subscriber("/root/{{braced}}")
async def handle(body: str) -> None:
    print(body)


prefix = "/root"


@broker.subscriber(f"{prefix}/{{{{braced}}}}/status")
async def handle_status(body: str) -> None:
    print(body)
