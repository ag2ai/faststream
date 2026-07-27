import uuid
from typing import Any

import pytest

from faststream import BaseMiddleware
from faststream.sqs.broker.broker import SQSBroker
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

    def _with_response_queue(self, kwargs: dict[str, Any]) -> None:
        # SQS has no native RPC; a per-broker reply queue is required.
        kwargs.setdefault("response_queue", f"responses-{uuid.uuid4().hex}")


@pytest.mark.connected()
@pytest.mark.sqs()
class TestRealRequests(SQSTestcaseConfig, SQSRequestsTestcase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> SQSBroker:
        self._with_response_queue(kwargs)
        return super().get_broker(apply_types=apply_types, **kwargs)


@pytest.mark.sqs()
class TestRequestTestClient(SQSMemoryTestcaseConfig, SQSRequestsTestcase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> SQSBroker:
        self._with_response_queue(kwargs)
        return super().get_broker(apply_types=apply_types, **kwargs)
