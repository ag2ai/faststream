from fastapi import FastAPI
from faststream.kafka.fastapi import KafkaRouter

core_router = KafkaRouter()
nested_router = KafkaRouter()

@core_router.subscriber("core-topic")
async def handler():
    ...

@nested_router.subscriber("nested-topic")
async def nested_handler():
    ...

core_router.include_router(nested_router)

app = FastAPI(lifespan=core_router.lifespan_context)
app.include_router(core_router)
