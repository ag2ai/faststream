from faststream import FastStream, Depends
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)

def simple_dependency() -> int:
    return 1

@broker.subscriber("test")
async def handler(body: dict, d: int = Depends(simple_dependency)):
    assert d == 1
