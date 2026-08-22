from typing import Any

import pytest
from typing_extensions import override

from faststream._internal.utils.path import Address
from tests.brokers.base.address import AddressCheckTestcase

from .basic import MQTTMemoryTestcaseConfig


@pytest.mark.mqtt()
class TestMQTTAddressTemplate(MQTTMemoryTestcaseConfig, AddressCheckTestcase):
    # An MQTT Path parameter occupies a whole topic level, so the family is
    # spelled with `/` rather than `.`.
    template = "logs/{level}"
    broker_address = "logs/+"
    literal = "logs/info"
    broken_template = "logs/${ENV"

    @override
    def get_subscriber_address(self, subscriber: Any) -> Address:
        return subscriber.address

    def test_an_escaped_brace_is_a_literal_brace_not_a_template(self) -> None:
        broker = self.get_broker()

        subscriber = self.declare_subscriber(broker, "logs/{{raw}}/{level}")

        address = self.get_subscriber_address(subscriber)
        assert address.template == "logs/{{raw}}/{level}"
        assert address.broker_address == "logs/{raw}/+"
