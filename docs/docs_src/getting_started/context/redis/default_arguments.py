from typing import Annotated
from faststream import Context, FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)

@broker.subscriber("test-channel")
async def handle(
    not_existed: Annotated[None, Context("not_existed", default=None)],
):
    assert not_existed is None
