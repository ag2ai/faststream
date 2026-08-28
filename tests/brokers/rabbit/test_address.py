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


@pytest.mark.rabbit()
class TestAddressDelivery(RabbitMemoryTestcaseConfig, RabbitAddressDelivery):
    pass


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestAddressDeliveryReal(RabbitTestcaseConfig, RabbitAddressDelivery):
    pass
