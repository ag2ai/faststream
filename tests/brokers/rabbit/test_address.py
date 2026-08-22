from typing import Any

import pytest
from typing_extensions import override

from faststream._internal.utils.path import Address
from faststream.rabbit import RabbitQueue
from tests.brokers.base.address import AddressCheckTestcase

from .basic import RabbitMemoryTestcaseConfig


@pytest.mark.rabbit()
class TestRabbitAddressTemplate(RabbitMemoryTestcaseConfig, AddressCheckTestcase):
    broker_address = "logs.*"

    @override
    def declare_subscriber(self, obj: Any, template: str) -> Any:
        return obj.subscriber(RabbitQueue("test", routing_key=template))

    @override
    def get_subscriber_address(self, subscriber: Any) -> Address:
        # `queue` reads through ticket 08's layer, which composes the prefix.
        return subscriber.queue.routing_address


@pytest.mark.rabbit()
def test_both_reads_fall_back_to_the_queue_name() -> None:
    queue = RabbitQueue("test")

    assert queue.routing_template() == "test"
    assert queue.routing() == "test"


@pytest.mark.rabbit()
def test_a_declared_routing_key_keeps_both_reads() -> None:
    queue = RabbitQueue("test", routing_key="logs.{level}")

    assert queue.routing_template() == "logs.{level}"
    assert queue.routing() == "logs.*"
