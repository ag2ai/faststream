from faststream import FastStream
from faststream.mqtt import MQTTBroker, MQTTMessage, QoS

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.subscriber("rpc/echo", qos=QoS.AT_LEAST_ONCE)
async def echo(body: str) -> str:
    return body


@app.after_startup
async def demo() -> None:
    reply: MQTTMessage = await broker.request("hello", "rpc/echo", timeout=5.0)
    print(reply.body)
