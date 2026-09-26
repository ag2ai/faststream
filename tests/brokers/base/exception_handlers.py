from typing import Any
from unittest.mock import AsyncMock

import pytest

from tests.brokers.base.basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class ExceptionHandlersTestcase(BaseTestcaseConfig[Any]):
    @pytest.mark.parametrize(
        ("local_result", "broker_result", "expected_result", "expected_calls"),
        (
            (True, False, True, ["subscriber"]),
            (False, True, True, ["subscriber", "broker"]),
            (False, False, False, ["subscriber", "broker"]),
        ),
    )
    async def test_exception_handler_chain(
        self,
        queue: str,
        local_result: bool,
        broker_result: bool,
        expected_result: bool,
        expected_calls: list[str],
    ) -> None:
        error = ValueError("test error")
        calls: list[tuple[str, BaseException]] = []

        def local_handler(exc: BaseException) -> bool:
            calls.append(("subscriber", exc))
            return local_result

        async def broker_handler(exc: BaseException) -> bool:
            calls.append(("broker", exc))
            return broker_result

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(
            *args,
            **kwargs,
            exception_handler=local_handler,
        )

        result = await subscriber._handle_exception(error)

        assert (result, calls) == (
            expected_result,
            [(name, error) for name in expected_calls],
        )

    async def test_consume_calls_exception_handler(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = ValueError("message processing failed")
        received_errors: list[BaseException] = []

        async def exception_handler(exc: BaseException) -> bool:
            received_errors.append(exc)
            return True

        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(
            *args,
            **kwargs,
            exception_handler=exception_handler,
        )

        monkeypatch.setattr(subscriber, "running", True)
        monkeypatch.setattr(
            subscriber,
            "process_message",
            AsyncMock(side_effect=error),
        )

        await subscriber.consume(object())

        assert received_errors == [error]
