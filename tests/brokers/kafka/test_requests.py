from typing import Any

import pytest

from faststream import BaseMiddleware
from tests.brokers.base.requests import RequestsTestcase

from .basic import KafkaMemoryTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        self.msg.value *= 2

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class TestRequestTestClient(KafkaMemoryTestcaseConfig, RequestsTestcase):
    def get_middleware(self, **kwargs: Any):
        return Mid
