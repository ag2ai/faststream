from typing import Any

import pytest
from typing_extensions import override

from faststream._internal.utils.path import Address
from tests.brokers.base.address import AddressTemplateTestcase

from .basic import NatsTestcaseConfig


@pytest.mark.nats()
class TestNatsAddressTemplate(NatsTestcaseConfig, AddressTemplateTestcase):
    broker_address = "logs.*"

    @override
    def get_subscriber_address(self, subscriber: Any) -> Address:
        return subscriber.subject

    def test_publisher_reads_through_the_same_address(self) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="prefix_")

        publisher = router.publisher("out.{id}")
        broker.include_router(router)

        assert publisher.subject.template == "prefix_out.{id}"
        assert publisher.subject.broker_address == "prefix_out.*"

    def test_escaped_braces_are_literal(self) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber("cache{{shard}}")

        assert subscriber.subject.template == "cache{{shard}}"
        assert subscriber.subject.broker_address == "cache{shard}"
