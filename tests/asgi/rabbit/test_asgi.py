from typing import Any

import pytest

from faststream.rabbit import RabbitBroker, TestRabbitBroker
from tests.asgi.testcase import AsgiTestcase


@pytest.mark.rabbit()
class TestRabbitAsgi(AsgiTestcase):
    def get_broker(self, **kwargs: Any) -> RabbitBroker:
        return RabbitBroker(**kwargs)

    def get_test_broker(self, broker: RabbitBroker) -> TestRabbitBroker:
        return TestRabbitBroker(broker)
