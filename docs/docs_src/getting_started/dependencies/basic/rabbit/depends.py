from faststream import FastStream, Depends
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

def simple_dependency() -> int:
    return 1

@broker.subscriber("test")
async def handler(body: dict, d: int = Depends(simple_dependency)):
    assert d == 1
