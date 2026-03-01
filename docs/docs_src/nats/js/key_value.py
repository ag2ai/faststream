from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream(broker)


@broker.subscriber("key", kv_watch="bucket")
async def handler(msg: str):
    assert msg == "Hello!"


@app.after_startup
async def setup_broker():
    key_value = await broker.key_value(bucket="bucket")
    await key_value.put("key", b"Hello!")
