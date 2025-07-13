from contextlib import asynccontextmanager

from fastapi import FastAPI

from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")

@broker.subscriber("test")
async def base_handler(body):
    print(body)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.start()
    yield
    await broker.stop()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"Hello": "World"}
