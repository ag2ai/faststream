from typing import Any

import pytest
from typing_extensions import override

from tests.brokers.base.address import AddressDeliveryTestcase

from .basic import KafkaMemoryTestcaseConfig


@pytest.mark.kafka()
class TestAddressDelivery(KafkaMemoryTestcaseConfig, AddressDeliveryTestcase):
    """Kafka compiles an address only behind `pattern=`; a topic is a literal.

    `pattern=` is a Subscriber argument, so a Kafka Publisher never holds a
    template and there is no round trip to assert. The pattern half runs in
    memory only: a real consumer discovers a pattern's topics on a metadata
    refresh, which is minutes by default and is not what this is testing.
    """

    @override
    def declare_subscriber(self, obj: Any, declaration: str, queue: str) -> Any:
        return obj.subscriber(pattern=declaration)
