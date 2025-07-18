import pytest

from typing import Any

from faststream.redis import RedisBroker, TestRedisBroker
from tests.asgi.testcase import AsgiTestcase


@pytest.mark.redis()
class TestRedisAsgi(AsgiTestcase):
    def get_broker(self, **kwargs: Any) -> RedisBroker:
        return RedisBroker(**kwargs)

    def get_test_broker(self, broker: RedisBroker) -> TestRedisBroker:
        return TestRedisBroker(broker)
