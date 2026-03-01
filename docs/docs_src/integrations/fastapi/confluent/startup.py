from fastapi import FastAPI

from faststream.confluent.fastapi import KafkaRouter

router = KafkaRouter("localhost:9092")


@router.subscriber("test")
async def hello(msg: str):
    return {"response": "Hello, Kafka!"}


@router.after_startup
async def test(app: FastAPI):
    await router.broker.publish("Hello!", "test")


app = FastAPI()
app.include_router(router)
