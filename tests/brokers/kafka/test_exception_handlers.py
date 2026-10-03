import asyncio
from typing import Any
from unittest.mock import MagicMock, call

import pytest

from tests.brokers.base.exception_handlers import ExceptionHandlersTestcase

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
class TestExceptionHandlers(KafkaTestcaseConfig, ExceptionHandlersTestcase):
    @pytest.mark.connected()
    async def test_subscriber_exception_handler_false_propagates_error(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def exception_handler(exc: BaseException) -> bool:
            mock(exc)
            return False

        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=exception_handler,
        )
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber  # type: ignore[untyped-decorator]
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            await br.start()
            (consume_task,) = subscriber.tasks

            await br.publish("hello", queue)

            with pytest.raises(
                RuntimeError,
                match="message processing failed",
            ) as exc_info:
                await asyncio.wait_for(
                    consume_task,
                    timeout=self.timeout,
                )

        assert exc_info.value is error
        mock.assert_called_once_with(error)

    @pytest.mark.connected()
    async def test_broker_exception_handler_false_propagates_error(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def exception_handler(exc: BaseException) -> bool:
            mock(exc)
            return False

        broker = self.get_broker(exception_handler=exception_handler)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber  # type: ignore[untyped-decorator]
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            await br.start()
            (consume_task,) = subscriber.tasks

            await br.publish("hello", queue)

            with pytest.raises(
                RuntimeError,
                match="message processing failed",
            ) as exc_info:
                await asyncio.wait_for(
                    consume_task,
                    timeout=self.timeout,
                )

        assert exc_info.value is error
        mock.assert_called_once_with(error)

    @pytest.mark.connected()
    async def test_error_propagates_when_both_handlers_decline(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        def local_handler(exc: BaseException) -> bool:
            mock("subscriber", exc)
            return False

        def broker_handler(exc: BaseException) -> bool:
            mock("broker", exc)
            return False

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber  # type: ignore[untyped-decorator]
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            await br.start()
            (consume_task,) = subscriber.tasks

            await br.publish("hello", queue)

            with pytest.raises(
                RuntimeError,
                match="message processing failed",
            ) as exc_info:
                await asyncio.wait_for(
                    consume_task,
                    timeout=self.timeout,
                )

        assert exc_info.value is error
        assert mock.call_args_list == [
            call("subscriber", error),
            call("broker", error),
        ]
