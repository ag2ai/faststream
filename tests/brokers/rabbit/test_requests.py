from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from faststream import BaseMiddleware
from tests.brokers.base.requests import RequestsTestcase

from .basic import RabbitMemoryTestcaseConfig, RabbitTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        assert self.msg
        self.msg._Message__lock = False
        self.msg.body *= 2

    async def consume_scope(
        self, call_next: Callable[[Any], Awaitable[Any]], msg: Any
    ) -> Any:
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class RabbitRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs: Any) -> Any:
        return Mid


# A reply stream left open warns only when it is collected
@pytest.mark.filterwarnings("error::ResourceWarning")
@pytest.mark.filterwarnings("error::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.connected()
@pytest.mark.rabbit()
class TestRealRequests(RabbitTestcaseConfig, RabbitRequestsTestcase):
    pass


@pytest.mark.rabbit()
@pytest.mark.asyncio()
class TestRequestTestClient(RabbitMemoryTestcaseConfig, RabbitRequestsTestcase):
    pass
