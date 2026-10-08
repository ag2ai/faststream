from typing import Annotated
from faststream import Context, ContextRepo, FastStream, UnderlyingDriverAnnotation
from faststream.nats import NatsBroker

class Storage:
    async def save(self, data: str) -> None: ...

StorageClient = Annotated[Storage, Context("storage")]

broker = NatsBroker(
    "nats://localhost:4222",
    underlying_driver_annotations={
        Storage: UnderlyingDriverAnnotation(
            type_hint=StorageClient,
            module="app.dependencies",
            name="StorageClient",
        ),
    },
)
app = FastStream(broker)

@broker.subscriber("test-subject")
async def handle(msg: str, storage: StorageClient):
    await storage.save(msg)

@app.on_startup
async def set_storage(context: ContextRepo):
    context.set_global("storage", Storage())
