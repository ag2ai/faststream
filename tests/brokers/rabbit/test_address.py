from typing import Any

import pytest
from typing_extensions import override

from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue
from tests.brokers.base.address import AddressPublisherDeliveryTestcase

from .basic import RabbitMemoryTestcaseConfig, RabbitTestcaseConfig

EXCHANGE = RabbitExchange("address-tests", type=ExchangeType.TOPIC)


class RabbitAddressDelivery(AddressPublisherDeliveryTestcase):
    """RabbitMQ addresses a message by routing key, over a topic exchange.

    The queue is named separately from the key it binds with, which is why every
    declaration here goes through `RabbitQueue` rather than a bare string.
    """

    @override
    def declare_subscriber(self, obj: Any, declaration: str, queue: str) -> Any:
        return obj.subscriber(RabbitQueue(queue, routing_key=declaration), EXCHANGE)

    @override
    def declare_publisher(self, obj: Any, declaration: str, queue: str) -> Any:
        return obj.publisher(routing_key=declaration, exchange=EXCHANGE)

    @override
    async def publish(self, broker: Any, address: str, message: str) -> None:
        await broker.publish(message, routing_key=address, exchange=EXCHANGE)

    # A router prefix decorates the queue name as well as the routing key, and
    # `pamqp`, the frame encoder under `aio-pika`, matches a queue name against
    # `^[a-zA-Z0-9-_.:@#,/ ]*$` before it will encode a Queue.Declare. A brace
    # fails that and raises `ValueError: Invalid value for queue` from inside the
    # encoder, at `start()`, naming no subscriber.
    #
    # The block is the client's, not the broker's: RabbitMQ stores any UTF-8 name
    # up to 255 bytes, and declares `probe{{v1}}.braces` without complaint. So
    # these two describe a subscriber FastStream cannot currently declare — which
    # is the deferred question about whether a prefix should reach a queue name at
    # all, and it is about every character pamqp rejects, not about braces.
    @pytest.mark.skip(reason="pamqp rejects a brace in a queue name, client-side")
    @override
    async def test_a_router_prefix_is_a_declaration_too(self, *args: Any) -> None: ...

    @pytest.mark.skip(reason="pamqp rejects a brace in a queue name, client-side")
    @override
    async def test_a_router_prefix_reaches_both_ends_of_a_declaration(
        self,
        *args: Any,
    ) -> None: ...


@pytest.mark.rabbit()
class TestAddressDelivery(RabbitMemoryTestcaseConfig, RabbitAddressDelivery):
    pass


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestAddressDeliveryReal(RabbitTestcaseConfig, RabbitAddressDelivery):
    pass
