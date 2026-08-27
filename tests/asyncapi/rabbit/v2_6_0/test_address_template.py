from typing import Any

import pytest

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from tests.asyncapi.base.v2_6_0.address_template import AddressTemplateTestcase

EXCHANGE = RabbitExchange("logs-ex", type=ExchangeType.TOPIC)


@pytest.mark.rabbit()
class TestAddressTemplate(AddressTemplateTestcase):
    broker_class = RabbitBroker

    def declare_subscriber(self, broker: Any) -> None:
        broker.subscriber(
            RabbitQueue("logs-q", routing_key=self.address_template),
            EXCHANGE,
        )

    def declare_publisher(self, broker: Any) -> None:
        broker.publisher(
            RabbitQueue("logs-q", routing_key=self.address_template),
            EXCHANGE,
        )
