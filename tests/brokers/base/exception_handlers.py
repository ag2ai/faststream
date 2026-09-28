from typing import Any
from unittest.mock import AsyncMock

import pytest

from faststream import Context
from tests.brokers.base.basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class ExceptionHandlersTestcase(BaseTestcaseConfig[Any]):
    async def test_subscriber_exception_handler_true_suppresses_error(
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

        subscriber._build_fastdepends_model()
        monkeypatch.setattr(subscriber, "running", True)
        monkeypatch.setattr(
            subscriber,
            "process_message",
            AsyncMock(side_effect=error),
        )

        await subscriber.consume(object())

        assert received_errors == [error]

    async def test_subscriber_exception_handler_false_propagates_error(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = ValueError("message processing failed")
        received_errors: list[BaseException] = []

        async def exception_handler(exc: BaseException) -> bool:
            received_errors.append(exc)
            return False

        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(
            *args,
            **kwargs,
            exception_handler=exception_handler,
        )

        subscriber._build_fastdepends_model()
        monkeypatch.setattr(subscriber, "running", True)
        monkeypatch.setattr(
            subscriber,
            "process_message",
            AsyncMock(side_effect=error),
        )

        with pytest.raises(
            ValueError,
            match="message processing failed",
        ) as exc_info:
            await subscriber.consume(object())

        assert exc_info.value is error
        assert received_errors == [error]

    async def test_subscriber_exception_handler_receives_context(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = ValueError("message processing failed")
        expected_service = object()
        received: list[tuple[BaseException, object]] = []

        async def exception_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            received.append((exc, service))
            return True

        broker = self.get_broker(apply_types=True)
        broker.context.set_global("error_service", expected_service)

        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(
            *args,
            **kwargs,
            exception_handler=exception_handler,
        )

        subscriber._build_fastdepends_model()
        monkeypatch.setattr(subscriber, "running", True)
        monkeypatch.setattr(
            subscriber,
            "process_message",
            AsyncMock(side_effect=error),
        )

        await subscriber.consume(object())

        assert received == [(error, expected_service)]

    async def test_subscriber_handles_error_without_calling_broker(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = ValueError("message processing failed")
        calls: list[tuple[str, BaseException]] = []

        async def local_handler(exc: BaseException) -> bool:
            calls.append(("subscriber", exc))
            return True

        async def broker_handler(exc: BaseException) -> bool:
            calls.append(("broker", exc))
            return False

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(
            *args,
            **kwargs,
            exception_handler=local_handler,
        )

        subscriber._build_fastdepends_model()
        monkeypatch.setattr(subscriber, "running", True)
        monkeypatch.setattr(
            subscriber,
            "process_message",
            AsyncMock(side_effect=error),
        )

        await subscriber.consume(object())

        assert calls == [("subscriber", error)]

    async def test_broker_handles_error_after_subscriber_declines(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = ValueError("message processing failed")
        calls: list[str] = []

        def local_handler(exc: BaseException) -> bool:
            calls.append("subscriber")
            return False

        def broker_handler(exc: BaseException) -> bool:
            calls.append("broker")
            return True

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(
            *args,
            **kwargs,
            exception_handler=local_handler,
        )

        subscriber._build_fastdepends_model()
        monkeypatch.setattr(subscriber, "running", True)
        monkeypatch.setattr(
            subscriber,
            "process_message",
            AsyncMock(side_effect=error),
        )

        await subscriber.consume(object())

        assert calls == ["subscriber", "broker"]

    async def test_error_propagates_when_both_handlers_decline(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = ValueError("message processing failed")
        calls: list[tuple[str, BaseException]] = []

        async def local_handler(exc: BaseException) -> bool:
            calls.append(("subscriber", exc))
            return False

        async def broker_handler(exc: BaseException) -> bool:
            calls.append(("broker", exc))
            return False

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(
            *args,
            **kwargs,
            exception_handler=local_handler,
        )

        subscriber._build_fastdepends_model()
        monkeypatch.setattr(subscriber, "running", True)
        monkeypatch.setattr(
            subscriber,
            "process_message",
            AsyncMock(side_effect=error),
        )

        with pytest.raises(
            ValueError,
            match="message processing failed",
        ) as exc_info:
            await subscriber.consume(object())

        assert exc_info.value is error
        assert calls == [
            ("subscriber", error),
            ("broker", error),
        ]

    async def test_broker_exception_handler_false_propagates_error(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = ValueError("message processing failed")
        received_errors: list[BaseException] = []

        async def exception_handler(exc: BaseException) -> bool:
            received_errors.append(exc)
            return False

        broker = self.get_broker(exception_handler=exception_handler)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        subscriber._build_fastdepends_model()
        monkeypatch.setattr(subscriber, "running", True)
        monkeypatch.setattr(
            subscriber,
            "process_message",
            AsyncMock(side_effect=error),
        )

        with pytest.raises(
            ValueError,
            match="message processing failed",
        ) as exc_info:
            await subscriber.consume(object())

        assert exc_info.value is error
        assert received_errors == [error]

    async def test_broker_exception_handler_true_suppresses_error(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = ValueError("message processing failed")
        received_errors: list[BaseException] = []

        async def exception_handler(exc: BaseException) -> bool:
            received_errors.append(exc)
            return True

        broker = self.get_broker(exception_handler=exception_handler)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        subscriber._build_fastdepends_model()
        monkeypatch.setattr(subscriber, "running", True)
        monkeypatch.setattr(
            subscriber,
            "process_message",
            AsyncMock(side_effect=error),
        )

        await subscriber.consume(object())

        assert received_errors == [error]

    async def test_broker_exception_handler_receives_context(
        self,
        queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = ValueError("message processing failed")
        expected_service = object()
        received: list[tuple[BaseException, object]] = []

        async def exception_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            received.append((exc, service))
            return True

        broker = self.get_broker(
            apply_types=True,
            exception_handler=exception_handler,
        )
        broker.context.set_global("error_service", expected_service)

        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        subscriber._build_fastdepends_model()
        monkeypatch.setattr(subscriber, "running", True)
        monkeypatch.setattr(
            subscriber,
            "process_message",
            AsyncMock(side_effect=error),
        )

        await subscriber.consume(object())

        assert received == [(error, expected_service)]
