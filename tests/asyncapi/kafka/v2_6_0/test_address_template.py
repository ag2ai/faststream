from typing import Any

import pytest

from faststream.kafka import KafkaBroker
from tests.asyncapi.base.v2_6_0.address_template import AddressTemplateTestcase


@pytest.mark.kafka()
class TestAddressTemplate(AddressTemplateTestcase):
    """Kafka takes a template through `pattern=`, and only on a Subscriber.

    A Publisher's topic is a literal Kafka never compiles, so its half of this
    testcase pins that the topic reaches the document unmangled.
    """

    broker_class = KafkaBroker

    broker_address = "logs..*"

    def declare_subscriber(self, broker: Any) -> None:
        broker.subscriber(pattern=self.address_template)
