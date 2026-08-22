"""MQTT-specific `skip_none` behavior: retained messages.

In MQTT, publishing a zero-length payload with `retain=True` removes the
retained message from the topic. FastStream publishes `None` as an empty
payload, so by default such a publish clears the retained message; with
`skip_none=True` the `None` is never sent and the retained message stays.
"""

from faststream import FastStream, Logger
from faststream.mqtt import MQTTBroker

broker = MQTTBroker()
app = FastStream(broker)

# without `skip_none`: `None` is published as an empty payload,
# which clears the retained message
publisher = broker.publisher("device/status", retain=True)

# with `skip_none=True`: `None` is skipped, the retained message survives
publisher_keep = broker.publisher("device/status", retain=True, skip_none=True)


@broker.subscriber("device/status")
async def handle_status(msg: bytes, logger: Logger) -> None:
    logger.info("Status: %r", msg)


@app.after_startup
async def test_publishing() -> None:
    await publisher.publish("online")

    # `None` -> empty payload + `retain=True`: the retained "online"
    # message is cleared, new subscribers no longer receive it
    await publisher.publish(None)

    await publisher.publish("online")

    # skipped entirely: the retained "online" message stays
    await publisher_keep.publish(None)
