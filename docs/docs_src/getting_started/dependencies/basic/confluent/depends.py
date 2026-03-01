from faststream import FastStream, Depends
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

def simple_dependency() -> int:
    return 1

@broker.subscriber("test")
async def handler(body: dict, d: int = Depends(simple_dependency)):
    assert d == 1
