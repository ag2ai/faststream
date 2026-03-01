from fastapi import Depends, FastAPI
from typing_extensions import Annotated

from faststream.confluent import KafkaBroker, fastapi

router = fastapi.KafkaRouter("localhost:9092")

app = FastAPI()


def broker():
    return router.broker


@router.get("/")
async def hello_http(broker: Annotated[KafkaBroker, Depends(broker)]):
    await broker.publish("Hello, Kafka!", "test")
    return "Hello, HTTP!"


app.include_router(router)
