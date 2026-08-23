"""The parser a Subscriber consumes through is its Broker's version, not the default.

A Subscriber is built before it knows its Broker — a Router-declared one before
`include_router` runs at all — so it has no parser at all until Preparation, which
is the first moment the version is in scope. Preparation is also where the chain the
handler consumes through is composed, so building any later composes the wrong parser
in: the Subscriber holds the 3.1.1 parser and reads messages with the 5.0 one.

Read through `call.parser` rather than the Subscriber's own attribute: that is the
callable a message actually travels, and the pair only disagree when the resolution
lands too late to matter.
"""

from typing import Any, Literal

import pytest
import zmqtt

from faststream.mqtt import MQTTBroker, MQTTRouter

pytestmark = pytest.mark.mqtt()

TOPIC = "sensors/temp"


def a_message_carrying_v5_properties() -> zmqtt.Message:
    """What a 5.0 broker delivers and a 3.1.1 one has no way to.

    The two parsers differ in whether they read this envelope, which makes it a
    direct reading of which one is composed in.
    """
    return zmqtt.Message(
        topic=TOPIC,
        payload=b"body",
        qos=zmqtt.QoS(0),
        retain=False,
        properties=zmqtt.PublishProperties(
            content_type="text/plain",
            response_topic="reply/here",
            correlation_data=b"correlation",
            user_properties=(("header", "value"),),
        ),
    )


def broker_with_a_router_subscriber(version: Literal["3.1.1", "5.0"]) -> MQTTBroker:
    broker = MQTTBroker(version=version)
    router = MQTTRouter()

    @router.subscriber(TOPIC)
    async def handler(msg: Any) -> None: ...

    broker.include_router(router)
    return broker


async def parse_through(broker: MQTTBroker, msg: zmqtt.Message) -> Any:
    (subscriber,) = broker.subscribers
    (call,) = subscriber.calls
    assert call.parser is not None
    return await call.parser(msg)


@pytest.mark.asyncio()
async def test_a_router_subscriber_on_a_311_broker_consumes_as_311() -> None:
    broker = broker_with_a_router_subscriber("3.1.1")

    broker.prepare()

    parsed = await parse_through(broker, a_message_carrying_v5_properties())

    assert parsed.headers == {}
    assert parsed.reply_to == ""
    assert parsed.content_type is None
    # Not `is None`: a message that arrives without correlation data is given a
    # fresh id, so what says the 5.0 envelope went unread is that the id is a new
    # one rather than the value on the wire.
    assert parsed.correlation_id
    assert parsed.correlation_id != "correlation"


@pytest.mark.asyncio()
async def test_a_router_subscriber_on_a_50_broker_consumes_as_50() -> None:
    broker = broker_with_a_router_subscriber("5.0")

    broker.prepare()

    parsed = await parse_through(broker, a_message_carrying_v5_properties())

    assert parsed.headers == {"header": "value"}
    assert parsed.reply_to == "reply/here"
    assert parsed.correlation_id == "correlation"
    assert parsed.content_type == "text/plain"
