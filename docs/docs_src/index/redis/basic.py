from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)

@broker.subscriber("in-channel")
@broker.publisher("out-channel")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
