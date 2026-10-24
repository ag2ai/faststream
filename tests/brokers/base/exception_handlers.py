from typing import Any
from unittest.mock import MagicMock, call

import pytest

from faststream import Context, FastStream, TestApp
from faststream.exceptions import StopApplication, StopConsume
from faststream.message import AckStatus, StreamMessage
from faststream.middlewares import AckPolicy
from tests.brokers.base.basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class ExceptionHandlersTestcase(BaseTestcaseConfig[Any]):
    async def test_exception_handlers_are_not_called_for_successful_message(
        self,
        queue: str,
        mock: MagicMock,
        mock2: MagicMock,
    ) -> None:
        async def exception_handler(exc: BaseException) -> bool:
            mock(exc)
            return True

        broker = self.get_broker(exception_handler=exception_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=exception_handler,
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            mock2(msg)

        app = FastStream(broker, exception_handler=exception_handler)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            await br.publish("hello", queue)

        mock2.assert_called_once_with("hello")
        mock.assert_not_called()

    @pytest.mark.parametrize("error_type", (StopConsume, StopApplication))
    async def test_exception_handlers_do_not_suppress_stop_commands(
        self,
        queue: str,
        mock: MagicMock,
        error_type: type[BaseException],
    ) -> None:
        error = error_type()

        async def exception_handler(exc: BaseException) -> bool:
            mock(exc)
            return True

        broker = self.get_broker(exception_handler=exception_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=exception_handler,
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(broker, exception_handler=exception_handler)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            with pytest.raises(error_type) as exc_info:
                await br.publish(None, queue)

        assert exc_info.value is error
        mock.assert_not_called()

    async def test_subscriber_exception_handler_true_suppresses_error(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def exception_handler(exc: BaseException) -> bool:
            mock(exc)
            return True

        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=exception_handler,
        )
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            await br.publish(None, queue)

        mock.assert_called_once_with(error)

    async def test_subscriber_exception_handler_receives_context(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        expected_service = object()

        async def exception_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock(exc, service)
            return True

        broker = self.get_broker(apply_types=True)
        broker.context.set_global("error_service", expected_service)

        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=exception_handler,
        )
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            await br.publish(None, queue)

        mock.assert_called_once_with(error, expected_service)

    async def test_subscriber_handles_error_without_calling_broker(
        self,
        queue: str,
        mock: MagicMock,
        mock2: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def local_handler(exc: BaseException) -> bool:
            mock(exc)
            return True

        async def broker_handler(exc: BaseException) -> bool:
            mock2(exc)
            return False

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            await br.publish(None, queue)

        mock.assert_called_once_with(error)
        mock2.assert_not_called()

    async def test_broker_handles_error_after_subscriber_declines(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def local_handler(exc: BaseException) -> bool:
            mock("subscriber", exc)
            return False

        async def broker_handler(exc: BaseException) -> bool:
            mock("broker", exc)
            return True

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            await br.publish(None, queue)

        assert mock.call_args_list == [
            call("subscriber", error),
            call("broker", error),
        ]

    async def test_broker_exception_handler_true_suppresses_error(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def exception_handler(exc: BaseException) -> bool:
            mock(exc)
            return True

        broker = self.get_broker(exception_handler=exception_handler)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            await br.publish(None, queue)

        mock.assert_called_once_with(error)

    async def test_broker_exception_handler_receives_context(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        expected_service = object()

        async def exception_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock(exc, service)
            return True

        broker = self.get_broker(
            apply_types=True,
            exception_handler=exception_handler,
        )
        broker.context.set_global("error_service", expected_service)

        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            await br.publish(None, queue)

        mock.assert_called_once_with(error, expected_service)

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

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            with pytest.raises(
                RuntimeError,
                match="message processing failed",
            ) as exc_info:
                await br.publish(None, queue)

        mock.assert_called_once_with(error)
        assert exc_info.value is error

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

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            with pytest.raises(
                RuntimeError,
                match="message processing failed",
            ) as exc_info:
                await br.publish(None, queue)

        assert mock.call_args_list == [
            call("subscriber", error),
            call("broker", error),
        ]
        assert exc_info.value is error

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

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        async with self.patch_broker(broker) as br:
            with pytest.raises(
                RuntimeError,
                match="message processing failed",
            ) as exc_info:
                await br.publish(None, queue)

        mock.assert_called_once_with(error)
        assert exc_info.value is error

    async def test_broker_exception_handlers_are_independent(
        self,
        queue: str,
        mock: MagicMock,
        mock2: MagicMock,
    ) -> None:
        error1 = RuntimeError("first broker processing failed")
        error2 = RuntimeError("second broker processing failed")

        async def exception_handler1(exc: BaseException) -> bool:
            mock(exc)
            return True

        async def exception_handler2(exc: BaseException) -> bool:
            mock2(exc)
            return True

        broker1 = self.get_broker(exception_handler=exception_handler1)
        broker2 = self.get_broker(exception_handler=exception_handler2)

        args, kwargs = self.get_subscriber_params(queue)
        args2, kwargs2 = self.get_subscriber_params(queue + "1")
        subscriber1 = broker1.subscriber(*args, **kwargs)
        subscriber2 = broker2.subscriber(*args2, **kwargs2)

        @subscriber1
        async def handler1(msg: Any) -> None:
            raise error1

        @subscriber2
        async def handler2(msg: Any) -> None:
            raise error2

        app = FastStream(broker1, broker2)

        async with (
            self.patch_broker(broker1, connect_only=True) as br1,
            self.patch_broker(broker2, connect_only=True) as br2,
            TestApp(app),
        ):
            await br1.publish(None, queue)

            mock2.assert_not_called()

            await br2.publish(None, queue + "1")

        mock.assert_called_once_with(error1)
        mock2.assert_called_once_with(error2)

    @pytest.mark.parametrize("sync_handler", (False, True))
    async def test_app_exception_handler_true_suppresses_error(
        self,
        queue: str,
        mock: MagicMock,
        sync_handler: bool,
    ) -> None:
        error = RuntimeError("message processing failed")

        def sync_exception_handler(exc: BaseException) -> bool:
            mock(exc)
            return True

        async def async_exception_handler(exc: BaseException) -> bool:
            mock(exc)
            return True

        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(
            broker,
            exception_handler=(
                sync_exception_handler if sync_handler else async_exception_handler
            ),
        )

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            await br.publish(None, queue)

        mock.assert_called_once_with(error)

    async def test_app_exception_handler_false_propagates_error(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def exception_handler(exc: BaseException) -> bool:
            mock(exc)
            return False

        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(broker, exception_handler=exception_handler)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            with pytest.raises(
                RuntimeError,
                match="message processing failed",
            ) as exc_info:
                await br.publish(None, queue)

        mock.assert_called_once_with(error)
        assert exc_info.value is error

    async def test_app_handles_error_after_subscriber_and_broker_decline(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def local_handler(exc: BaseException) -> bool:
            mock("subscriber", exc)
            return False

        async def broker_handler(exc: BaseException) -> bool:
            mock("broker", exc)
            return False

        async def app_handler(exc: BaseException) -> bool:
            mock("app", exc)
            return True

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(broker, exception_handler=app_handler)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            await br.publish(None, queue)

        assert mock.call_args_list == [
            call("subscriber", error),
            call("broker", error),
            call("app", error),
        ]

    async def test_app_handler_is_not_called_after_subscriber_handles_error(
        self,
        queue: str,
        mock: MagicMock,
        mock2: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def local_handler(exc: BaseException) -> bool:
            mock("subscriber", exc)
            return True

        async def broker_handler(exc: BaseException) -> bool:
            mock("broker", exc)
            return True

        async def app_handler(exc: BaseException) -> bool:
            mock2(exc)
            return True

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(broker, exception_handler=app_handler)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            await br.publish(None, queue)

        assert mock.call_args_list == [call("subscriber", error)]
        mock2.assert_not_called()

    async def test_app_handler_is_not_called_after_broker_handles_error(
        self,
        queue: str,
        mock: MagicMock,
        mock2: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")

        async def local_handler(exc: BaseException) -> bool:
            mock("subscriber", exc)
            return False

        async def broker_handler(exc: BaseException) -> bool:
            mock("broker", exc)
            return True

        async def app_handler(exc: BaseException) -> bool:
            mock2(exc)
            return True

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(broker, exception_handler=app_handler)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            await br.publish(None, queue)

        assert mock.call_args_list == [
            call("subscriber", error),
            call("broker", error),
        ]
        mock2.assert_not_called()

    async def test_error_propagates_when_all_three_handlers_decline(
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

        def app_handler(exc: BaseException) -> bool:
            mock("app", exc)
            return False

        broker = self.get_broker(exception_handler=broker_handler)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(broker, exception_handler=app_handler)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            with pytest.raises(
                RuntimeError,
                match="message processing failed",
            ) as exc_info:
                await br.publish(None, queue)

        assert mock.call_args_list == [
            call("subscriber", error),
            call("broker", error),
            call("app", error),
        ]
        assert exc_info.value is error

    async def test_app_exception_handler_receives_context(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        expected_service = object()

        async def exception_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock(exc, service)
            return True

        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(broker, exception_handler=exception_handler)
        app.context.set_global("error_service", expected_service)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            await br.publish(None, queue)

        mock.assert_called_once_with(error, expected_service)

    async def test_app_exception_handlers_are_independent(
        self,
        queue: str,
        mock: MagicMock,
        mock2: MagicMock,
    ) -> None:
        error1 = RuntimeError("first app processing failed")
        error2 = RuntimeError("second app processing failed")

        async def exception_handler1(exc: BaseException) -> bool:
            mock(exc)
            return True

        async def exception_handler2(exc: BaseException) -> bool:
            mock2(exc)
            return True

        broker1, broker2 = self.get_broker(), self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)
        args2, kwargs2 = self.get_subscriber_params(queue + "1")
        subscriber1 = broker1.subscriber(*args, **kwargs)
        subscriber2 = broker2.subscriber(*args2, **kwargs2)

        @subscriber1
        async def handler1(msg: Any) -> None:
            raise error1

        @subscriber2
        async def handler2(msg: Any) -> None:
            raise error2

        app1 = FastStream(broker1, exception_handler=exception_handler1)
        app2 = FastStream(broker2, exception_handler=exception_handler2)

        async with (
            self.patch_broker(broker1, connect_only=True) as br1,
            self.patch_broker(broker2, connect_only=True) as br2,
            TestApp(app1),
            TestApp(app2),
        ):
            await br1.publish(None, queue)
            mock2.assert_not_called()

            await br2.publish(None, queue + "1")

        mock.assert_called_once_with(error1)
        mock2.assert_called_once_with(error2)

    async def test_app_without_exception_handlers_propagates_error(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            mock(msg)
            raise error

        app = FastStream(broker)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            with pytest.raises(
                RuntimeError,
                match="message processing failed",
            ) as exc_info:
                await br.publish("hello", queue)

        mock.assert_called_once_with("hello")
        assert exc_info.value is error

    async def test_subscriber_exception_handler_receives_dependency_from_app_context(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        expected_service = object()

        async def exception_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock(exc, service)
            return True

        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=exception_handler,
        )

        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(broker)
        app.context.set_global("error_service", expected_service)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            await br.publish(None, queue)

        mock.assert_called_once_with(error, expected_service)

    async def test_broker_exception_handler_receives_dependency_from_app_context(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        expected_service = object()

        async def exception_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock(exc, service)
            return True

        broker = self.get_broker(
            apply_types=True,
            exception_handler=exception_handler,
        )
        args, kwargs = self.get_subscriber_params(queue)

        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(broker)
        app.context.set_global("error_service", expected_service)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            await br.publish(None, queue)

        mock.assert_called_once_with(error, expected_service)

    async def test_app_exception_handler_receives_each_broker_context(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error1 = RuntimeError("first broker processing failed")
        error2 = RuntimeError("second broker processing failed")

        async def exception_handler(
            exc: BaseException,
            current_broker: Any = Context("broker"),
        ) -> bool:
            mock(exc, current_broker)
            return True

        broker1 = self.get_broker(apply_types=True)
        broker2 = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(queue)
        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @broker1.subscriber(*args, **kwargs)
        async def handler1(msg: Any) -> None:
            raise error1

        @broker2.subscriber(*args2, **kwargs2)
        async def handler2(msg: Any) -> None:
            raise error2

        app = FastStream(broker1, broker2, exception_handler=exception_handler)

        async with (
            self.patch_broker(broker1, connect_only=True) as br1,
            self.patch_broker(broker2, connect_only=True) as br2,
            TestApp(app),
        ):
            await br1.publish(None, queue)
            await br2.publish(None, queue + "1")

        assert mock.call_args_list == [
            call(error1, broker1),
            call(error2, broker2),
        ]

    async def test_exception_handlers_receive_context_after_broker_is_added(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        expected_service = object()

        async def local_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock("subscriber", exc, service)
            return False

        async def broker_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock("broker", exc, service)
            return False

        async def app_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock("app", exc, service)
            return True

        broker = self.get_broker(
            apply_types=True,
            exception_handler=broker_handler,
        )
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            raise error

        app = FastStream(exception_handler=app_handler)
        app.context.set_global("error_service", expected_service)
        app.add_broker(broker)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            await br.publish(None, queue)

        assert mock.call_args_list == [
            call("subscriber", error, expected_service),
            call("broker", error, expected_service),
            call("app", error, expected_service),
        ]

    async def test_exception_handlers_receive_context_after_router_is_included(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        expected_service = object()

        async def local_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock("subscriber", exc, service)
            return False

        async def broker_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock("broker", exc, service)
            return False

        async def app_handler(
            exc: BaseException,
            service: object = Context("error_service"),
        ) -> bool:
            mock("app", exc, service)
            return True

        broker = self.get_broker(apply_types=True, exception_handler=broker_handler)
        app = FastStream(broker, exception_handler=app_handler)
        app.context.set_global("error_service", expected_service)
        router = self.get_router()
        args, kwargs = self.get_subscriber_params(
            queue,
            exception_handler=local_handler,
        )

        @router.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            raise error

        broker.include_router(router)

        async with self.patch_broker(broker, connect_only=True) as br, TestApp(app):
            await br.publish(None, queue)

        assert mock.call_args_list == [
            call("subscriber", error, expected_service),
            call("broker", error, expected_service),
            call("app", error, expected_service),
        ]

    async def test_handled_error_is_suppressed_before_acknowledgement(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        messages: list[StreamMessage[Any]] = []

        async def exception_handler(
            exc: BaseException,
            message: StreamMessage[Any] = Context("message"),
        ) -> bool:
            mock(exc, message.committed)
            return True

        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(
            queue,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            exception_handler=exception_handler,
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(
            msg: Any,
            message: StreamMessage[Any] = Context("message"),
        ) -> None:
            messages.append(message)
            raise error

        async with self.patch_broker(broker) as br:
            await br.publish(None, queue)

        assert (mock.call_args_list, [message.committed for message in messages]) == (
            [call(error, None)],
            [AckStatus.ACKED],
        )

    async def test_unhandled_error_is_propagated_after_negative_acknowledgement(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        error = RuntimeError("message processing failed")
        messages: list[StreamMessage[Any]] = []

        async def exception_handler(
            exc: BaseException,
            message: StreamMessage[Any] = Context("message"),
        ) -> bool:
            mock(exc, message.committed)
            return False

        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(
            queue,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            exception_handler=exception_handler,
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(
            msg: Any,
            message: StreamMessage[Any] = Context("message"),
        ) -> None:
            messages.append(message)
            raise error

        async with self.patch_broker(broker) as br:
            with pytest.raises(RuntimeError) as exc_info:
                await br.publish(None, queue)

        assert (
            mock.call_args_list,
            [message.committed for message in messages],
            exc_info.value,
        ) == ([call(error, None)], [AckStatus.NACKED], error)
