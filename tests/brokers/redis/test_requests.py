import pytest

from faststream import BaseMiddleware
from faststream.redis import BinaryMessageFormatV1
from tests.brokers.base.requests import RequestsTestcase

from .basic import RedisMemoryTestcaseConfig, RedisTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        data, headers = BinaryMessageFormatV1.parse(self.msg["data"])
        data *= 2
        self.msg["data"] = BinaryMessageFormatV1.encode(
            message=data,
            reply_to=None,
            correlation_id=headers["correlation_id"],
            headers=headers,
        )

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class RedisRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs):
        return Mid


@pytest.mark.connected()
@pytest.mark.redis()
class TestRealRequests(RedisTestcaseConfig, RedisRequestsTestcase):
    pass


@pytest.mark.redis()
class TestRequestTestClient(RedisMemoryTestcaseConfig, RedisRequestsTestcase):
    pass
