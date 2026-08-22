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
