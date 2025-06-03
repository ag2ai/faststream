from faststream import FastStream, Depends
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)

def another_dependency() -> int:
    return 1

def simple_dependency(b: int = Depends(another_dependency)) -> int: # (1)
    return b * 2

@broker.subscriber("test")
async def handler(
    body: dict,
    a: int = Depends(another_dependency),
    b: int = Depends(simple_dependency),
):
    assert a + b == 3
