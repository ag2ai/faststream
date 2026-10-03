import asyncio
from typing import Any
from unittest.mock import MagicMock, call

import pytest

from faststream import Context
from tests.brokers.base.basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class ExceptionHandlersTestcase(BaseTestcaseConfig[Any]):
    @pytest.mark.connected()
    async def test_subscriber_exception_handler_true_suppresses_error(
        self,
        queue: str,
        event: asyncio.Event,
        event2: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def exception_handler(exc: BaseException) -> bool:
            mock(exc)
            event.set()
            return True

        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=exception_handler,
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            if msg == "hello":
                raise error
            event2.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await br.publish("hello", queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

            await br.publish("next", queue)
            await asyncio.wait_for(event2.wait(), timeout=self.timeout)

        mock.assert_called_once_with(error)

    @pytest.mark.connected()
    async def test_subscriber_exception_handler_receives_context(
        self,
        queue: str,
        event: asyncio.Event,
        event2: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        expected_service = object()

        async def exception_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock(exc, service)
            event.set()
            return True

        broker = self.get_broker(apply_types=True)
        broker.context.set_global("error_service", expected_service)

        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=exception_handler,
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            if msg == "hello":
                raise error
            event2.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await br.publish("hello", queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

            await br.publish("next", queue)
            await asyncio.wait_for(event2.wait(), timeout=self.timeout)

        mock.assert_called_once_with(error, expected_service)

    @pytest.mark.connected()
    async def test_subscriber_handles_error_without_calling_broker(
        self,
        queue: str,
        event: asyncio.Event,
        event2: asyncio.Event,
        mock: MagicMock,
        mock2: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def local_handler(exc: BaseException) -> bool:
            mock(exc)
            event.set()
            return True

        async def broker_handler(exc: BaseException) -> bool:
            mock2(exc)
            return False

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            if msg == "hello":
                raise error
            event2.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await br.publish("hello", queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

            await br.publish("next", queue)
            await asyncio.wait_for(event2.wait(), timeout=self.timeout)

        mock.assert_called_once_with(error)
        mock2.assert_not_called()

    @pytest.mark.connected()
    async def test_broker_handles_error_after_subscriber_declines(
        self,
        queue: str,
        event: asyncio.Event,
        event2: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def local_handler(exc: BaseException) -> bool:
            mock("subscriber", exc)
            return False

        async def broker_handler(exc: BaseException) -> bool:
            mock("broker", exc)
            event.set()
            return True

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            if msg == "hello":
                raise error
            event2.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await br.publish("hello", queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

            await br.publish("next", queue)
            await asyncio.wait_for(event2.wait(), timeout=self.timeout)

        assert mock.call_args_list == [
            call("subscriber", error),
            call("broker", error),
        ]

    @pytest.mark.connected()
    async def test_broker_exception_handler_true_suppresses_error(
        self,
        queue: str,
        event: asyncio.Event,
        event2: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def exception_handler(exc: BaseException) -> bool:
            mock(exc)
            event.set()
            return True

        broker = self.get_broker(exception_handler=exception_handler)
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            if msg == "hello":
                raise error
            event2.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await br.publish("hello", queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

            await br.publish("next", queue)
            await asyncio.wait_for(event2.wait(), timeout=self.timeout)

        mock.assert_called_once_with(error)

    @pytest.mark.connected()
    async def test_broker_exception_handler_receives_context(
        self,
        queue: str,
        event: asyncio.Event,
        event2: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        expected_service = object()

        async def exception_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock(exc, service)
            event.set()
            return True

        broker = self.get_broker(
            apply_types=True,
            exception_handler=exception_handler,
        )
        broker.context.set_global("error_service", expected_service)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            if msg == "hello":
                raise error
            event2.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await br.publish("hello", queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

            await br.publish("next", queue)
            await asyncio.wait_for(event2.wait(), timeout=self.timeout)

        mock.assert_called_once_with(error, expected_service)
