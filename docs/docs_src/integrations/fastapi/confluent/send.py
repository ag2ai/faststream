from fastapi import FastAPI

from faststream.confluent.fastapi import KafkaRouter

router = KafkaRouter("localhost:9092")

app = FastAPI()


@router.get("/")
async def hello_http():
    await router.broker.publish("Hello, Kafka!", "test")
    return "Hello, HTTP!"


app.include_router(router)
