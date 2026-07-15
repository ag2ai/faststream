from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", version="3.1.1")
app = FastStream(broker)


@broker.subscriber("rpc/manual-reply/echo")
async def manual_reply_echo(body: str) -> str:
    return body


@app.after_startup
async def demo() -> None:
    reply = await broker.request(
        "hello",
        "rpc/manual-reply/echo",
        reply_to="rpc/manual-reply/replies",
        timeout=5.0,
    )
    print(reply.body)
