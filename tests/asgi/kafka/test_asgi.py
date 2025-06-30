from typing import Any

from faststream.kafka import KafkaBroker, TestKafkaBroker
from tests.asgi.testcase import AsgiTestcase


class TestKafkaAsgi(AsgiTestcase):
    def get_broker(self, **kwargs: Any) -> KafkaBroker:
        return KafkaBroker(**kwargs)

    def get_test_broker(self, broker: KafkaBroker) -> TestKafkaBroker:
        return TestKafkaBroker(broker)
