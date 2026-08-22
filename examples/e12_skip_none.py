"""General `skip_none` example.

Works the same way for every broker's publisher: a `None` handler return
value is not published, and `request(None)` returns `None` without sending
a request to the broker. See the per-broker examples for broker-specific
behavior (e.g. Kafka tombstones).
"""

from faststream import FastStream
from faststream.annotations import Logger
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

publisher = broker.publisher("response-queue", skip_none=True)


@publisher
@broker.subscriber("test-queue")
async def handle(msg: str, logger: Logger) -> str | None:
    logger.info(msg)
    if msg == "ignore":
        # `None` is not published: no message reaches "response-queue"
        return None

    return f"Processed: {msg}"


@broker.subscriber("response-queue")
async def handle_response(msg: str, logger: Logger) -> None:
    logger.info("Process response: %s", msg)


@app.after_startup
async def test_publishing() -> None:
    await broker.publish("Hello!", "test-queue")  # -> "Processed: Hello!" is published
    await broker.publish("ignore", "test-queue")  # -> nothing is published

    # `request(None)` returns `None` without sending a request to the broker
    response = await publisher.request(None)
    assert response is None
