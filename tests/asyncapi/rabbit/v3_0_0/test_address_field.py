import pytest

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema

TOPIC = RabbitExchange("test-ex", type=ExchangeType.TOPIC)
FANOUT = RabbitExchange("test-ex", type=ExchangeType.FANOUT)


def _addresses(broker: RabbitBroker) -> dict[str, str | None]:
    return {
        name: channel.get("address")
        for name, channel in get_3_0_0_schema(broker)["channels"].items()
    }


@pytest.mark.rabbit()
def test_the_routing_key_is_the_address_not_the_queue() -> None:
    """A queue is a place to read from; the routing key is what addresses a message.

    It is also the only one of RabbitMQ's three names that can hold a `{param}`,
    so a queue called one thing and bound by another renders the second.
    """
    broker = RabbitBroker()

    @broker.subscriber(RabbitQueue("test-q", routing_key="logs.{level}"), TOPIC)
    async def handle_logs(body: str) -> None: ...

    broker.publisher(RabbitQueue("test-q", routing_key="cache"), TOPIC)

    assert _addresses(broker) == {
        "test-q:test-ex:HandleLogs": "logs.{level}",
        "cache:test-ex:Publisher": "cache",
    }


@pytest.mark.rabbit()
def test_a_fanout_has_no_address() -> None:
    """A fanout reaches every queue bound to it, so no key routes a message here.

    The document already drops the queue binding for this case; rendering the queue
    name as the address would put back exactly what that omission removed. Absent
    rather than empty: AsyncAPI reads a missing address as unknown, `""` does not.
    """
    broker = RabbitBroker()

    @broker.subscriber(RabbitQueue("test-q"), FANOUT)
    async def handle(body: str) -> None: ...

    broker.publisher(RabbitQueue("test-q"), FANOUT)

    assert _addresses(broker) == {
        "test-q:test-ex:Handle": None,
        "_:test-ex:Publisher": None,
    }


@pytest.mark.rabbit()
def test_a_routing_key_declared_on_a_fanout_is_still_no_address() -> None:
    """The exchange decides, not the declaration.

    A fanout ignores any routing key handed to it, so one declared anyway still
    addresses nothing — and a publisher must not read an address where the
    subscriber on the far side of it reads none.
    """
    broker = RabbitBroker()
    broker.publisher(RabbitQueue("test-q"), FANOUT, routing_key="ignored")

    assert _addresses(broker) == {"ignored:test-ex:Publisher": None}
