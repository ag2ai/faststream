from fastapi import Depends, FastAPI
from pydantic import BaseModel

from faststream.confluent.fastapi import KafkaRouter, Logger

router = KafkaRouter("localhost:9092")

class Incoming(BaseModel):
    m: dict

def call():
    return True

@router.subscriber("test")
@router.publisher("response")
async def hello(m: Incoming, logger: Logger, d=Depends(call)):
    logger.info("%s", m)
    return {"response": "Hello, Kafka!"}

@router.get("/")
async def hello_http():
    return "Hello, HTTP!"

app = FastAPI()
app.include_router(router)
