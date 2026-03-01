from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any

from faststream import BaseMiddleware, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage


class TopLevelMiddleware(BaseMiddleware):
    async def on_receive(self) -> None:
        print(f"call toplevel middleware with msg: {self.msg}")
        return await super().on_receive()

    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool:
        print("highlevel middleware out")
        return await super().after_processed(exc_type, exc_val, exc_tb)


async def subscriber_middleware(
    call_next: Callable[[Any], Awaitable[Any]],
    msg: RabbitMessage,
) -> Any:
    print(f"call handler middleware with body: {msg}")
    msg.body = b"fake message"
    result = await call_next(msg)
    print("handler middleware out")
    return result


broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",
    middlewares=(TopLevelMiddleware,),
)
app = FastStream(broker)


@broker.subscriber("test", middlewares=(subscriber_middleware,))
async def handle(msg: str) -> None:
    assert msg == "fake message"


@app.after_startup
async def test_publish() -> None:
    await broker.publish("message", "test")
