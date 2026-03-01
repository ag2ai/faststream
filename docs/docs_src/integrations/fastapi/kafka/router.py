from fastapi import FastAPI
from faststream.kafka import KafkaRouter
from faststream.kafka.fastapi import KafkaRouter as StreamRouter

core_router = StreamRouter()
nested_router = KafkaRouter()

@core_router.subscriber("core-topic")
async def handler():
    ...

@nested_router.subscriber("nested-topic")
async def nested_handler():
    ...

core_router.include_router(nested_router)

app = FastAPI()
app.include_router(core_router)
