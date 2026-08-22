from typing import Any

import pytest

from faststream.mqtt import MQTTBroker
from tests.asyncapi.base.v3_0_0.address_field import AddressFieldTestcase

SHARED_TOPIC = "$share/group/test"


@pytest.mark.mqtt()
class TestAddressField(AddressFieldTestcase):
    broker_class = MQTTBroker


@pytest.mark.mqtt()
class TestSharedSubscriptionAddressField(AddressFieldTestcase):
    """`$share/<group>/` asks for a shared subscription; it is not part of a topic.

    A message delivered to this subscriber carries `test`, never
    `$share/group/test`, so that is what the address says. The group is not lost
    from the document: the channel name keeps it, and so does `bindings.mqtt.topic`.
    """

    broker_class = MQTTBroker

    def declare_subscriber(self, broker: Any) -> None:
        broker.subscriber(self.address, shared="group")

    def test_the_group_stays_in_the_name_and_the_binding(self) -> None:
        key, channel = self.subscriber_channel()

        # 3.0 channel keys are cleaned of the characters a `$ref` cannot hold,
        # which is why the name reads `$share.group.test` rather than the topic
        assert key.startswith("$share.group.test")
        assert channel["bindings"]["mqtt"]["topic"] == SHARED_TOPIC

    @pytest.mark.skip(reason="A publisher cannot join a shared subscription.")
    def test_publisher_address_is_the_address(self) -> None: ...
