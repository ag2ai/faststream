from typing import Any

import pytest

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from tests.asyncapi.base.v3_0_0.address_field import AddressFieldTestcase

TOPIC_EXCHANGE = RabbitExchange("test-ex", type=ExchangeType.TOPIC)
FANOUT_EXCHANGE = RabbitExchange("test-ex", type=ExchangeType.FANOUT)


@pytest.mark.rabbit()
class TestAddressField(AddressFieldTestcase):
    broker_class = RabbitBroker


@pytest.mark.rabbit()
class TestRoutingKeyIsTheAddress(AddressFieldTestcase):
    """The routing key, not the queue name and not `exchange/routing_key`.

    A queue is a place a consumer reads from; the routing key is the string a
    message is addressed with, and the only one of RabbitMQ's three names that can
    hold a `{param}`. So a queue called one thing and bound by another renders the
    second.
    """

    broker_class = RabbitBroker

    def declare_subscriber(self, broker: Any) -> None:
        broker.subscriber(
            RabbitQueue("test-q", routing_key=self.address),
            TOPIC_EXCHANGE,
        )

    def declare_publisher(self, broker: Any) -> None:
        broker.publisher(
            RabbitQueue("test-q", routing_key=self.address),
            TOPIC_EXCHANGE,
        )


@pytest.mark.rabbit()
class TestFanoutHasNoAddress(AddressFieldTestcase):
    """A fanout reaches every queue bound to it, so no key routes a message here.

    The document already drops the queue binding for this case; rendering the
    queue name as the address would put back exactly what that omission removed.
    """

    broker_class = RabbitBroker

    rendered_address = ""

    def declare_subscriber(self, broker: Any) -> None:
        broker.subscriber(RabbitQueue("test-q"), FANOUT_EXCHANGE)

    def declare_publisher(self, broker: Any) -> None:
        broker.publisher(RabbitQueue("test-q"), FANOUT_EXCHANGE)

    def test_a_declared_routing_key_is_still_no_address(self) -> None:
        """The exchange decides, not the declaration.

        A fanout ignores any routing key handed to it, so declaring one addresses
        nothing — and the publisher must not read it as an address while the
        subscriber on the far side of it reads none.
        """
        broker = self.broker_class()
        broker.publisher(
            RabbitQueue("test-q"),
            FANOUT_EXCHANGE,
            routing_key="ignored",
        )

        _, channel = self.only_channel(broker)

        assert channel["address"] == ""
