from faststream import FastStream
from faststream.gcp import GCPBroker

broker = GCPBroker(project_id="your-project-id")
app = FastStream(broker)


@broker.subscriber("test-subscription", topic="test-topic")
async def handle():
    await broker.publish("Hi!", topic="another-topic")


@broker.subscriber("another-subscription", topic="another-topic")
async def handle_next(msg: str):
    assert msg == "Hi!"


@app.after_startup
async def test():
    await broker.publish("", topic="test-topic")
