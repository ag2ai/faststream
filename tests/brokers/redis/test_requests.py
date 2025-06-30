import json

import pytest

from faststream import BaseMiddleware
from tests.brokers.base.requests import RequestsTestcase

from .basic import RedisMemoryTestcaseConfig, RedisTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        data = json.loads(self.msg["data"])
        data["data"] *= 2
        self.msg["data"] = json.dumps(data)

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class RedisRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs):
        return Mid


@pytest.mark.redis()
class TestRealRequests(RedisTestcaseConfig, RedisRequestsTestcase):
    pass


class TestRequestTestClient(RedisMemoryTestcaseConfig, RedisRequestsTestcase):
    pass
