from fastapi import Depends, FastAPI
from pydantic import BaseModel

from faststream.confluent.fastapi import KafkaRouter, Logger

router = KafkaRouter("localhost:9092")

class Incoming(BaseModel):
    m: dict

def call() -> bool:
    return True

@router.subscriber("test")
@router.publisher("response")
async def hello(message: Incoming, logger: Logger, dependency: bool = Depends(call)):
    logger.info("Incoming value: %s, depends value: %s" % (message.m, dependency))
    return {"response": "Hello, Kafka!"}

@router.get("/")
async def hello_http():
    return "Hello, HTTP!"

app = FastAPI()
app.include_router(router)
