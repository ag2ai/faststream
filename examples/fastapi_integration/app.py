from fastapi import FastAPI

from faststream.rabbit.fastapi import Logger, RabbitRouter

router = RabbitRouter("amqp://guest:guest@localhost:5672/")
app = FastAPI()

publisher = router.publisher("response-q")


@publisher
@router.subscriber("test-q")
async def handler(user_id: int, logger: Logger) -> str:
    logger.info(user_id)
    return f"{user_id} created"


app.include_router(router)
