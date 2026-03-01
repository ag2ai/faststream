from pydantic import BaseModel, Field, PositiveInt
from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

class User(BaseModel):
    user: str = Field(..., examples=["John"])
    user_id: PositiveInt = Field(..., examples=["1"])

@broker.subscriber("in-queue")
@broker.publisher("out-queue")
async def handle_msg(data: User) -> str:
    return f"User: {data.user} - {data.user_id} registered"
