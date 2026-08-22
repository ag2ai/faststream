from typing import Any

import pytest

from faststream.mqtt import MQTTBroker
from tests.asyncapi.base.v2_6_0.address_field import AddressFieldTestcase


@pytest.mark.mqtt()
class TestAddressField(AddressFieldTestcase):
    broker_class = MQTTBroker


@pytest.mark.mqtt()
class TestSharedSubscriptionAddressField(AddressFieldTestcase):
    """A shared subscription gains no address field in 2.6 either."""

    broker_class = MQTTBroker

    def declare_subscriber(self, broker: Any) -> None:
        broker.subscriber(self.address, shared="group")

    @pytest.mark.skip(reason="A publisher cannot join a shared subscription.")
    def test_publisher_has_no_address_field(self) -> None: ...
