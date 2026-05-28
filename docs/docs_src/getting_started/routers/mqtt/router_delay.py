from faststream import FastStream
from faststream.mqtt import MQTTBroker
from faststream.mqtt import MQTTRouter, MQTTRoute, MQTTPublisher

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)


async def handle(name: str, user_id: int):
    assert name == "John"
    assert user_id == 1
    return "Hi!"


router = MQTTRouter(
    handlers=(
        MQTTRoute(
            handle,
            "test-topic",
            publishers=(
                MQTTPublisher("outer-topic"),
            ),
        ),
    ),
)

broker.include_router(router)


@app.after_startup
async def test():
    await broker.publish({"name": "John", "user_id": 1}, topic="test-topic")
