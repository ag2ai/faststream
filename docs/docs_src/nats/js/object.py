from io import BytesIO

from faststream import FastStream
from faststream import Logger
from faststream.nats import NatsBroker
from faststream.nats.annotations import ObjectStorage

broker = NatsBroker()
app = FastStream(broker)


@broker.subscriber("example-bucket", obj_watch=True)
async def handler(
    filename: str,
    storage: ObjectStorage,
    logger: Logger,
):
    assert filename == "file.txt"
    file = await storage.get(filename)
    logger.info(file.data)


@app.after_startup
async def test_send():
    object_storage = await broker.object_storage("example-bucket")
    await object_storage.put("file.txt", BytesIO(b"File mock"))
