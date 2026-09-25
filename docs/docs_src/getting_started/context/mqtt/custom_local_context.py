from typing import Any, Annotated
from faststream import Context, FastStream, BaseMiddleware
from faststream.mqtt import MQTTBroker
from faststream.mqtt.annotations import MQTTMessage
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

broker = MQTTBroker("localhost", port=1883, middlewares=[Middleware])
app = FastStream(broker)

@broker.subscriber("test-topic")
async def handle(
    message: MQTTMessage,  # get from the context too
    correlation_id: Annotated[str, Context()],
):
    assert correlation_id == message.correlation_id
