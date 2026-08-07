import pytest

from faststream import BaseMiddleware
from faststream.redis import BinaryMessageFormatV1
from tests.brokers.base.requests import RequestsTestcase

from .basic import RedisMemoryTestcaseConfig, RedisTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        data, headers = BinaryMessageFormatV1.parse(self.msg["data"])
        data *= 2

        from faststream.response.publish_type import PublishType
        from faststream.response.response import PublishCommand

        cmd = PublishCommand(
            body=data,
            destination="",
            correlation_id=headers["correlation_id"],
            headers=headers,
            _publish_type=PublishType.PUBLISH,
        )
        self.msg["data"] = await BinaryMessageFormatV1.encode(
            cmd=cmd,
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
