from typing import Any

import pytest
from typing_extensions import override

from faststream._internal.utils.path import Address
from tests.brokers.base.address import AddressTemplateTestcase

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
class TestKafkaAddressTemplate(KafkaTestcaseConfig, AddressTemplateTestcase):
    broker_address = "logs..*"

    @override
    def declare_subscriber(self, obj: Any, template: str) -> Any:
        return obj.subscriber(pattern=template)

    @override
    def get_subscriber_address(self, subscriber: Any) -> Address:
        return subscriber.pattern

    def test_a_topic_subscriber_has_no_pattern(self) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber("topic")

        assert subscriber.pattern is None
