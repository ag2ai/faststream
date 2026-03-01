from typing import Any

import pytest

from faststream.kafka import KafkaBroker, TestKafkaBroker
from tests.asgi.testcase import AsgiTestcase


@pytest.mark.kafka()
class TestKafkaAsgi(AsgiTestcase):
    def get_broker(self, **kwargs: Any) -> KafkaBroker:
        return KafkaBroker(**kwargs)

    def get_test_broker(self, broker: KafkaBroker) -> TestKafkaBroker:
        return TestKafkaBroker(broker)
