from fastapi import Depends, FastAPI
from pydantic import BaseModel

from faststream.nats.fastapi import NatsRouter, Logger

router = NatsRouter("nats://localhost:4222")

class Incoming(BaseModel):
    m: dict

def call() -> bool:
    return True

@router.subscriber("test")
@router.publisher("response")
async def hello(message: Incoming, logger: Logger, dependency: bool = Depends(call)):
    logger.info("Incoming value: %s, depends value: %s" % (message.m, dependency))
    return {"response": "Hello, NATS!"}

@router.get("/")
async def hello_http():
    return "Hello, HTTP!"

app = FastAPI()
app.include_router(router)
