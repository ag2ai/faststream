from io import BytesIO

from faststream import FastStream, Logger
from faststream.nats import NatsBroker
from faststream.nats.annotations import ObjectStorage

broker = NatsBroker()
app = FastStream(broker)


@broker.subscriber("example-bucket", obj_watch=True)
async def handler(filename: str, storage: ObjectStorage, logger: Logger) -> None:
    assert filename == "file.txt"
    file = await storage.get(filename)
    logger.info(file.data)


@app.after_startup
async def test_send() -> None:
    os = await broker.object_storage("example-bucket")
    await os.put("file.txt", BytesIO(b"File mock"))
