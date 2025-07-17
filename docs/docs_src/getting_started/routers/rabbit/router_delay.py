from faststream import FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit import RabbitRoute, RabbitRouter, RabbitPublisher

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


async def handle(name: str, user_id: int):
    assert name == "John"
    assert user_id == 1
    return "Hi!"


router = RabbitRouter(
    handlers=(
        RabbitRoute(
            handle,
            "test-queue",
            publishers=(
                RabbitPublisher("outer-queue"),
            ),
        ),
    ),
)

broker.include_router(router)


@app.after_startup
async def test():
    await broker.publish({"name": "John", "user_id": 1}, queue="test-queue")
