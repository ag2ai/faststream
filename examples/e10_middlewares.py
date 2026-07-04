from types import TracebackType

from faststream import BaseMiddleware, FastStream
from faststream.rabbit import RabbitBroker


class TopLevelMiddleware(BaseMiddleware):
    async def on_receive(self) -> None:
        print(f"call toplevel middleware with msg: {self.msg}")
        return await super().on_receive()

    async def consume_scope(self, call_next, msg):
        print(f"call handler middleware with body: {msg}")
        msg.body = b"fake message"
        return await call_next(msg)

    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool:
        print("highlevel middleware out")
        return await super().after_processed(exc_type, exc_val, exc_tb)


broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",
    middlewares=(TopLevelMiddleware,),
)
app = FastStream(broker)


@broker.subscriber("test")
async def handle(msg: str) -> None:
    assert msg == "fake message"


@app.after_startup
async def test_publish() -> None:
    await broker.publish("message", "test")
