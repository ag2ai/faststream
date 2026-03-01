from pydantic import BaseModel, Field, PositiveInt
from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)

class User(BaseModel):
    user: str = Field(..., examples=["John"])
    user_id: PositiveInt = Field(..., examples=["1"])

@broker.subscriber("in-channel")
@broker.publisher("out-channel")
async def handle_msg(data: User) -> str:
    return f"User: {data.user} - {data.user_id} registered"
