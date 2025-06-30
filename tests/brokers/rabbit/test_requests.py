import pytest

from faststream import BaseMiddleware
from tests.brokers.base.requests import RequestsTestcase

from .basic import RabbitMemoryTestcaseConfig, RabbitTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        self.msg._Message__lock = False
        self.msg.body *= 2

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class RabbitRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs):
        return Mid


@pytest.mark.rabbit()
class TestRealRequests(RabbitTestcaseConfig, RabbitRequestsTestcase):
    pass


@pytest.mark.asyncio()
class TestRequestTestClient(RabbitMemoryTestcaseConfig, RabbitRequestsTestcase):
    pass
