import pytest

from faststream import BaseMiddleware
from tests.brokers.base.requests import RequestsTestcase

from .basic import MQMemoryTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        self.msg._StreamMessage__decoded_caches.clear()
        self.msg.body *= 2

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class MQRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs):
        return Mid


@pytest.mark.mq()
class TestRequestTestClient(MQMemoryTestcaseConfig, MQRequestsTestcase):
    pass
