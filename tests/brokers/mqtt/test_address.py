from typing import Any

import pytest
from typing_extensions import override

from faststream._internal.utils.path import Address
from faststream.exceptions import IncorrectState
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

    @override
    def test_an_early_read_does_not_pin_an_outer_router_prefix(self) -> None:
        """The read MQTT refuses, in place of the one it used to defend against.

        A Router included into another Router composes a longer prefix than its
        endpoints saw when they were declared. Asked in between, an MQTT
        Subscriber says the read came too early instead of answering with an
        address derived from a composition that is not final.
        """
        broker = self.get_broker()
        outer = self.get_router(prefix="outer_")
        inner = self.get_router(prefix="inner_")

        subscriber = self.declare_subscriber(inner, self.template)

        with pytest.raises(IncorrectState, match="too early"):
            self.get_subscriber_address(subscriber)

        outer.include_router(inner)
        broker.include_router(outer)

        address = self.read_address(broker.subscribers[0])
        assert address.template == f"outer_inner_{self.template}"
        assert address.broker_address == f"outer_inner_{self.broker_address}"

    def test_an_escaped_brace_is_a_literal_brace_not_a_template(self) -> None:
        broker = self.get_broker()

        subscriber = self.declare_subscriber(broker, "logs/{{raw}}/{level}")

        address = self.read_address(subscriber)
        assert address.template == "logs/{{raw}}/{level}"
        assert address.broker_address == "logs/{raw}/+"
