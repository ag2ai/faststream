import asyncio
from unittest.mock import MagicMock, call

import pytest

from faststream import Context
from faststream._internal.basic_types import DecodedMessage
from faststream.exceptions import SkipMessage
from faststream.middlewares import BaseMiddleware, ExceptionMiddleware
from faststream.response import PublishCommand

from .basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class MiddlewaresOrderTestcase(BaseTestcaseConfig):
    async def test_broker_middleware_order(self, queue: str, mock: MagicMock):
        class InnerMiddleware(BaseMiddleware):
            async def __aenter__(self):
                mock.enter_inner()
                mock.enter("inner")

            async def __aexit__(self, *args):
                mock.exit_inner()
                mock.exit("inner")

            async def consume_scope(self, call_next, msg):
                mock.consume_inner()
                mock.sub("inner")
                return await call_next(msg)

            async def publish_scope(self, call_next, cmd):
                mock.publish_inner()
                mock.pub("inner")
                return await call_next(cmd)

        class OuterMiddleware(BaseMiddleware):
            async def __aenter__(self):
                mock.enter_outer()
                mock.enter("outer")

            async def __aexit__(self, *args):
                mock.exit_outer()
                mock.exit("outer")

            async def consume_scope(self, call_next, msg):
                mock.consume_outer()
                mock.sub("outer")
                return await call_next(msg)

            async def publish_scope(self, call_next, cmd):
                mock.publish_outer()
                mock.pub("outer")
                return await call_next(cmd)

        broker = self.get_broker(middlewares=[OuterMiddleware, InnerMiddleware])

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg):
            pass

        async with self.patch_broker(broker) as br:
            await br.publish(None, queue)

        mock.consume_inner.assert_called_once()
        mock.consume_outer.assert_called_once()
        mock.publish_inner.assert_called_once()
        mock.publish_outer.assert_called_once()
        mock.enter_inner.assert_called_once()
        mock.enter_outer.assert_called_once()
        mock.exit_inner.assert_called_once()
        mock.exit_outer.assert_called_once()

        assert [c.args[0] for c in mock.sub.call_args_list] == ["outer", "inner"]
        assert [c.args[0] for c in mock.pub.call_args_list] == ["outer", "inner"]
        assert [c.args[0] for c in mock.enter.call_args_list] == ["outer", "inner"]
        assert [c.args[0] for c in mock.exit.call_args_list] == ["inner", "outer"]

    async def test_publisher_middleware_order(self, queue: str, mock: MagicMock):
        class InnerMiddleware(BaseMiddleware):
            async def publish_scope(self, call_next, cmd):
                mock.publish_inner()
                mock("inner")
                return await call_next(cmd)

        class MiddleMiddleware(BaseMiddleware):
            async def publish_scope(self, call_next, cmd):
                mock.publish_middle()
                mock("middle")
                return await call_next(cmd)

        class OuterMiddleware(BaseMiddleware):
            async def publish_scope(self, call_next, cmd):
                mock.publish_outer()
                mock("outer")
                return await call_next(cmd)

        broker = self.get_broker(middlewares=[OuterMiddleware])
        publisher = broker.publisher(
            queue,
            middlewares=[
                MiddleMiddleware(None, context=None).publish_scope,
                InnerMiddleware(None, context=None).publish_scope,
            ],
        )

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg):
            pass

        async with self.patch_broker(broker):
            await publisher.publish(None, queue)

        mock.publish_inner.assert_called_once()
        mock.publish_middle.assert_called_once()
        mock.publish_outer.assert_called_once()

        assert [c.args[0] for c in mock.call_args_list] == ["outer", "middle", "inner"]

    async def test_publisher_with_router_middleware_order(
        self, queue: str, mock: MagicMock
    ):
        class InnerMiddleware(BaseMiddleware):
            async def publish_scope(self, call_next, cmd):
                mock.publish_inner()
                mock("inner")
                return await call_next(cmd)

        class MiddleMiddleware(BaseMiddleware):
            async def publish_scope(self, call_next, cmd):
                mock.publish_middle()
                mock("middle")
                return await call_next(cmd)

        class OuterMiddleware(BaseMiddleware):
            async def publish_scope(self, call_next, cmd):
                mock.publish_outer()
                mock("outer")
                return await call_next(cmd)

        broker = self.get_broker(middlewares=[OuterMiddleware])
        router = self.get_router(middlewares=[MiddleMiddleware])
        router2 = self.get_router(middlewares=[InnerMiddleware])

        publisher = router2.publisher(queue)

        args, kwargs = self.get_subscriber_params(queue)

        @router2.subscriber(*args, **kwargs)
        async def handler(msg):
            pass

        router.include_router(router2)
        broker.include_router(router)

        async with self.patch_broker(broker):
            await publisher.publish(None, queue)

        mock.publish_inner.assert_called_once()
        mock.publish_middle.assert_called_once()
        mock.publish_outer.assert_called_once()

        assert [c.args[0] for c in mock.call_args_list] == ["outer", "middle", "inner"]

    async def test_consume_middleware_order(self, queue: str, mock: MagicMock):
        class InnerMiddleware(BaseMiddleware):
            async def consume_scope(self, call_next, cmd):
                mock.consume_inner()
                mock("inner")
                return await call_next(cmd)

        class MiddleMiddleware(BaseMiddleware):
            async def consume_scope(self, call_next, cmd):
                mock.consume_middle()
                mock("middle")
                return await call_next(cmd)

        class OuterMiddleware(BaseMiddleware):
            async def consume_scope(self, call_next, cmd):
                mock.consume_outer()
                mock("outer")
                return await call_next(cmd)

        broker = self.get_broker(middlewares=[OuterMiddleware])

        args, kwargs = self.get_subscriber_params(
            queue,
            middlewares=[
                MiddleMiddleware(None, context=None).consume_scope,
                InnerMiddleware(None, context=None).consume_scope,
            ],
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(msg):
            pass

        async with self.patch_broker(broker) as br:
            await br.publish(None, queue)

        mock.consume_inner.assert_called_once()
        mock.consume_middle.assert_called_once()
        mock.consume_outer.assert_called_once()

        assert [c.args[0] for c in mock.call_args_list] == ["outer", "middle", "inner"]

    async def test_consume_with_middleware_order(self, queue: str, mock: MagicMock):
        class InnerMiddleware(BaseMiddleware):
            async def consume_scope(self, call_next, cmd):
                mock.consume_inner()
                mock("inner")
                return await call_next(cmd)

        class MiddleMiddleware(BaseMiddleware):
            async def consume_scope(self, call_next, cmd):
                mock.consume_middle()
                mock("middle")
                return await call_next(cmd)

        class OuterMiddleware(BaseMiddleware):
            async def consume_scope(self, call_next, cmd):
                mock.consume_outer()
                mock("outer")
                return await call_next(cmd)

        broker = self.get_broker(middlewares=[OuterMiddleware])
        router = self.get_router(middlewares=[MiddleMiddleware])
        router2 = self.get_router(middlewares=[InnerMiddleware])

        args, kwargs = self.get_subscriber_params(queue)

        @router2.subscriber(*args, **kwargs)
        async def handler(msg):
            pass

        router.include_router(router2)
        broker.include_router(router)
        async with self.patch_broker(broker) as br:
            await br.publish(None, queue)

        mock.consume_inner.assert_called_once()
        mock.consume_middle.assert_called_once()
        mock.consume_outer.assert_called_once()

        assert [c.args[0] for c in mock.call_args_list] == ["outer", "middle", "inner"]


@pytest.mark.asyncio()
class LocalMiddlewareTestcase(BaseTestcaseConfig):
    async def test_subscriber_middleware(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        async def mid(call_next, msg):
            mock.start(await msg.decode())
            result = await call_next(msg)
            mock.end()
            event.set()
            return result

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue, middlewares=(mid,))

        @broker.subscriber(*args, **kwargs)
        async def handler(m) -> str:
            mock.inner(m)
            return "end"

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("start", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        mock.start.assert_called_once_with("start")
        mock.inner.assert_called_once_with("start")

        assert event.is_set()
        mock.end.assert_called_once()

    async def test_publisher_middleware(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        async def mid(call_next, msg, **kwargs):
            mock.enter()
            result = await call_next(msg, **kwargs)
            mock.end()
            if mock.end.call_count > 1:
                event.set()
            return result

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        @broker.publisher(queue + "1", middlewares=(mid,))
        @broker.publisher(queue + "2", middlewares=(mid,))
        async def handler(m) -> str:
            mock.inner(m)
            return "end"

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("start", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.inner.assert_called_once_with("start")
        assert mock.enter.call_count == 2
        assert mock.end.call_count == 2

    async def test_local_middleware_not_shared_between_subscribers(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event1 = asyncio.Event()
        event2 = asyncio.Event()

        async def mid(call_next, msg):
            mock.start(msg)
            result = await call_next(msg)
            mock.end()
            return result

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)
        args2, kwargs2 = self.get_subscriber_params(
            queue + "1",
            middlewares=(mid,),
        )

        @broker.subscriber(*args, **kwargs)
        @broker.subscriber(*args2, **kwargs2)
        async def handler(m) -> str:
            if event1.is_set():
                event2.set()
            else:
                event1.set()
            mock()
            return ""

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("", queue)),
                    asyncio.create_task(broker.publish("", queue + "1")),
                    asyncio.create_task(event1.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event1.is_set()
        assert event2.is_set()
        mock.start.assert_called_once()
        mock.end.assert_called_once()
        assert mock.call_count == 2

    async def test_local_middleware_consume_not_shared_between_filters(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event1 = asyncio.Event()
        event2 = asyncio.Event()

        async def mid(call_next, msg):
            mock.start(msg)
            result = await call_next(msg)
            mock.end()
            return result

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            queue,
        )

        sub = broker.subscriber(*args, **kwargs)

        @sub(filter=lambda m: m.content_type == "application/json")
        async def handler(m) -> str:
            event2.set()
            mock()
            return ""

        @sub(middlewares=(mid,))
        async def handler2(m) -> str:
            event1.set()
            mock()
            return ""

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish({"msg": "hi"}, queue)),
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event1.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event1.is_set()
        assert event2.is_set()
        mock.start.assert_called_once()
        mock.end.assert_called_once()
        assert mock.call_count == 2

    async def test_error_traceback(self, queue: str, mock: MagicMock) -> None:
        event = asyncio.Event()

        async def mid(call_next, msg):
            try:
                result = await call_next(msg)
            except Exception as e:
                mock(isinstance(e, ValueError))
                raise
            else:
                return result

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue, middlewares=(mid,))

        @broker.subscriber(*args, **kwargs)
        async def handler2(m):
            event.set()
            raise ValueError

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with(True)


@pytest.mark.asyncio()
class MiddlewareTestcase(LocalMiddlewareTestcase):
    async def test_global_middleware(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        class mid(BaseMiddleware):  # noqa: N801
            async def on_receive(self):
                mock.start(self.msg)
                return await super().on_receive()

            async def after_processed(self, exc_type, exc_val, exc_tb):
                mock.end()
                return await super().after_processed(exc_type, exc_val, exc_tb)

        broker = self.get_broker(
            middlewares=(mid,),
        )

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(m) -> str:
            event.set()
            return ""

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )
        assert event.is_set()

        mock.start.assert_called_once()
        mock.end.assert_called_once()

    async def test_add_global_middleware(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        class mid(BaseMiddleware):  # noqa: N801
            async def on_receive(self):
                mock.start(self.msg)
                return await super().on_receive()

            async def after_processed(self, exc_type, exc_val, exc_tb):
                mock.end()
                return await super().after_processed(exc_type, exc_val, exc_tb)

        broker = self.get_broker()

        # already registered subscriber
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(m) -> str:
            event.set()
            return ""

        # should affect to already registered and a new subscriber both
        broker.add_middleware(mid)

        event2 = asyncio.Event()

        # new subscriber
        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @broker.subscriber(*args2, **kwargs2)
        async def handler2(m) -> str:
            event2.set()
            return ""

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(br.publish("", f"{queue}1")),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert mock.start.call_count == 2
        assert mock.end.call_count == 2

    async def test_patch_publish(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        class Mid(BaseMiddleware):
            async def on_publish(self, msg: PublishCommand) -> PublishCommand:
                msg.body *= 2
                return msg

        broker = self.get_broker(middlewares=(Mid,))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(m):
            return m

        args2, kwargs2 = self.get_subscriber_params(queue + "r")

        @broker.subscriber(*args2, **kwargs2)
        async def handler_resp(m) -> None:
            mock(m)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("r", queue, reply_to=queue + "r")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("rrrr")

    async def test_global_publisher_middleware(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        class Mid(BaseMiddleware):
            async def on_publish(self, msg: PublishCommand) -> PublishCommand:
                msg.body *= 2
                mock.enter(msg.body)
                return msg

            async def after_publish(self, *args, **kwargs) -> None:
                mock.end()
                if mock.end.call_count > 2:
                    event.set()

        broker = self.get_broker(middlewares=(Mid,))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        @broker.publisher(queue + "1")
        @broker.publisher(queue + "2")
        async def handler(m):
            mock.inner(m)
            return m

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("1", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.inner.assert_called_once_with("11")
        assert mock.enter.call_count == 3
        mock.enter.assert_called_with("1111")
        assert mock.end.call_count == 3


@pytest.mark.asyncio()
class ExceptionMiddlewareTestcase(BaseTestcaseConfig):
    async def test_exception_middleware_default_msg(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        mid = ExceptionMiddleware()

        @mid.add_handler(ValueError, publish=True)
        async def value_error_handler(exc) -> str:
            return "value"

        broker = self.get_broker(apply_types=True, middlewares=(mid,))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        @broker.publisher(queue + "1")
        async def subscriber1(m):
            raise ValueError

        args, kwargs = self.get_subscriber_params(queue + "1")

        @broker.subscriber(*args, **kwargs)
        async def subscriber2(msg=Context("message")) -> None:
            mock(await msg.decode())
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert mock.call_count == 1
        mock.assert_called_once_with("value")

    async def test_exception_middleware_skip_msg(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        mid = ExceptionMiddleware()

        @mid.add_handler(ValueError, publish=True)
        async def value_error_handler(exc):
            event.set()
            raise SkipMessage

        broker = self.get_broker(middlewares=(mid,))
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        @broker.publisher(queue + "1")
        async def subscriber1(m):
            raise ValueError

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @broker.subscriber(*args2, **kwargs2)
        async def subscriber2(msg=Context("message")) -> None:
            mock(await msg.decode())

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert mock.call_count == 0

    async def test_exception_middleware_do_not_catch_skip_msg(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        mid = ExceptionMiddleware()

        @mid.add_handler(Exception)
        async def value_error_handler(exc) -> None:
            mock()

        broker = self.get_broker(middlewares=(mid,))
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def subscriber(m):
            event.set()
            raise SkipMessage

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )
            await asyncio.sleep(0.001)

        assert event.is_set()
        assert mock.call_count == 0

    async def test_exception_middleware_reraise(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        mid = ExceptionMiddleware()

        @mid.add_handler(ValueError, publish=True)
        async def value_error_handler(exc):
            event.set()
            raise exc

        broker = self.get_broker(middlewares=(mid,))
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        @broker.publisher(queue + "1")
        async def subscriber1(m):
            raise ValueError

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @broker.subscriber(*args2, **kwargs2)
        async def subscriber2(msg=Context("message")) -> None:
            mock(await msg.decode())

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert mock.call_count == 0

    async def test_exception_middleware_different_handler(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        mid = ExceptionMiddleware()

        @mid.add_handler(ZeroDivisionError, publish=True)
        async def zero_error_handler(exc) -> str:
            return "zero"

        @mid.add_handler(ValueError, publish=True)
        async def value_error_handler(exc) -> str:
            return "value"

        broker = self.get_broker(apply_types=True, middlewares=(mid,))
        args, kwargs = self.get_subscriber_params(queue)

        publisher = broker.publisher(queue + "2")

        @broker.subscriber(*args, **kwargs)
        @publisher
        async def subscriber1(m):
            raise ZeroDivisionError

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @broker.subscriber(*args2, **kwargs2)
        @publisher
        async def subscriber2(m):
            raise ValueError

        args3, kwargs3 = self.get_subscriber_params(queue + "2")

        @broker.subscriber(*args3, **kwargs3)
        async def subscriber3(msg=Context("message")) -> None:
            mock(await msg.decode())
            if mock.call_count > 1:
                event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(br.publish("", queue + "1")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert mock.call_count == 2
        mock.assert_has_calls([call("zero"), call("value")], any_order=True)

    async def test_exception_middleware_init_handler_same(self) -> None:
        mid1 = ExceptionMiddleware()

        @mid1.add_handler(ValueError)
        async def value_error_handler(exc) -> str:
            return "value"

        mid2 = ExceptionMiddleware(handlers={ValueError: value_error_handler})

        assert [x[0] for x in mid1._handlers] == [x[0] for x in mid2._handlers]

    async def test_exception_middleware_init_publish_handler_same(self) -> None:
        mid1 = ExceptionMiddleware()

        @mid1.add_handler(ValueError, publish=True)
        async def value_error_handler(exc) -> str:
            return "value"

        mid2 = ExceptionMiddleware(publish_handlers={ValueError: value_error_handler})

        assert [x[0] for x in mid1._publish_handlers] == [
            x[0] for x in mid2._publish_handlers
        ]

    async def test_exception_middleware_decoder_error(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()

        async def decoder(
            msg,
            original_decoder,
        ) -> DecodedMessage:
            raise ValueError

        mid = ExceptionMiddleware()

        @mid.add_handler(ValueError)
        async def value_error_handler(exc) -> None:
            event.set()

        broker = self.get_broker(middlewares=(mid,), decoder=decoder)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def subscriber1(m):
            raise ZeroDivisionError

        async with self.patch_broker(broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
