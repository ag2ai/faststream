from typing import Any

import pytest

from faststream import BaseMiddleware
from tests.brokers.base.requests import RequestsTestcase

from .basic import SQSMemoryTestcaseConfig, SQSTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        self.msg["Body"] *= 2

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class SQSRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs: Any):
        return Mid


@pytest.mark.connected()
@pytest.mark.sqs()
class TestRealRequests(SQSTestcaseConfig, SQSRequestsTestcase):
    pass


@pytest.mark.sqs()
class TestRequestTestClient(SQSMemoryTestcaseConfig, SQSRequestsTestcase):
    pass
