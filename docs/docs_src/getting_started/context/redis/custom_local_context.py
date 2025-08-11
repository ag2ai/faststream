from typing import Any, Annotated
from faststream import Context, FastStream, BaseMiddleware
from faststream.redis import RedisBroker, RedisMessage
from faststream.types import AsyncFuncAny
from faststream.message import StreamMessage

class Middleware(BaseMiddleware):
    async def consume_scope(
        self,
        call_next: AsyncFuncAny,
        msg: StreamMessage[Any],
    ) -> Any:
        with self.context.scope("correlation_id", msg.correlation_id):
            return await super().consume_scope(call_next, msg)

broker = RedisBroker("redis://localhost:6379", middlewares=[Middleware])
app = FastStream(broker)

@broker.subscriber("test-channel")
async def handle(
    message: RedisMessage,  # get from the context too
    correlation_id: Annotated[str, Context()],
):
    assert correlation_id == message.correlation_id
