import pytest

from typing import Any

from faststream.nats import NatsBroker, TestNatsBroker
from tests.asgi.testcase import AsgiTestcase


@pytest.mark.nats()
class TestNatsAsgi(AsgiTestcase):
    def get_broker(self, **kwargs: Any) -> NatsBroker:
        return NatsBroker(**kwargs)

    def get_test_broker(self, broker: NatsBroker) -> TestNatsBroker:
        return TestNatsBroker(broker)
