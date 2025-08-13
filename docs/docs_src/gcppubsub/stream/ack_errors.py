from faststream import FastStream
from faststream.exceptions import AckMessage
from faststream.gcp import GCPBroker, StreamSub

broker = GCPBroker(project_id="test-project-id")
app = FastStream(broker)


@broker.subscriber(stream=StreamSub("test-stream", group="test-group", consumer="1"))
async def handle(body):
    processing_logic(body)


def processing_logic(body):
    if True:
        raise AckMessage()


@app.after_startup
async def test_publishing():
    await broker.publish("Hello World!", stream="test-stream")
