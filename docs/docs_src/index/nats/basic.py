from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)

@broker.subscriber("in-subject")
@broker.publisher("out-subject")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
