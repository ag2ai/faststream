from pydantic import BaseModel, Field, PositiveInt
from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)

class User(BaseModel):
    user: str = Field(..., examples=["John"])
    user_id: PositiveInt = Field(..., examples=["1"])

@broker.subscriber("in-subject")
@broker.publisher("out-subject")
async def handle_msg(data: User) -> str:
    return f"User: {data.user} - {data.user_id} registered"
