import asyncio
from unittest.mock import MagicMock, call

import anyio
import pytest
from pydantic import BaseModel

from faststream import Context, Depends, FastStream, TestApp
from faststream.context import ContextRepo
from faststream.exceptions import StopConsume

from .basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class MultibrokerTestcase(BaseTestcaseConfig):
    async def test_multi_consume(
        self, queue: str, mock: MagicMock, event: asyncio.Event, event2: asyncio.Event
    ) -> None:
        broker1, broker2 = self.get_broker(), self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)
        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @broker1.subscriber(*args, **kwargs)
        @broker2.subscriber(*args2, **kwargs2)
        def subscriber(m) -> None:
            mock()
            if mock.call_count == 1:
                event.set()
            else:
                event2.set()

        app = FastStream(broker1, broker2)

        async with (
            self.patch_broker(broker1) as br1,
            self.patch_broker(broker2) as br2,
            TestApp(app),
        ):
            await asyncio.wait(
                (
                    asyncio.create_task(br1.publish("hello", queue)),
                    asyncio.create_task(br2.publish("hello", queue + "1")),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2

    async def test_another_broker_publisher(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        broker1, broker2 = self.get_broker(), self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker1.subscriber(*args, **kwargs)
        @broker2.publisher(queue + "1")
        def subscriber(m):
            return m

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        # publisher sends message to the same broker
        @broker2.subscriber(*args2, **kwargs2)
        def subscriber2(m) -> None:
            mock(m)
            event.set()

        app = FastStream(broker1, broker2)

        async with (
            self.patch_broker(broker1) as br1,
            self.patch_broker(broker2),
            TestApp(app),
        ):
            await asyncio.wait(
                (
                    asyncio.create_task(br1.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("hello")

    async def test_crossbroker_publisher(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        broker1, broker2 = self.get_broker(), self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker1.subscriber(*args, **kwargs)
        @broker2.publisher(queue + "1")
        def subscriber(m):
            return m

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        # publisher sends message to another broker
        @broker1.subscriber(*args2, **kwargs2)
        def subscriber2(m) -> None:
            mock(m)
            event.set()

        app = FastStream(broker1, broker2)

        async with (
            self.patch_broker(broker1, broker2) as (br1, _),
            TestApp(app),
        ):
            await asyncio.wait(
                (
                    asyncio.create_task(br1.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("hello")


@pytest.mark.asyncio()
class BrokerConsumeTestcase(MultibrokerTestcase, BaseTestcaseConfig):
    async def test_consume(self, queue: str, event: asyncio.Event) -> None:
        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @consume_broker.subscriber(*args, **kwargs)
        def subscriber(m) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()

    async def test_consume_from_multi(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)
        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @consume_broker.subscriber(*args, **kwargs)
        @consume_broker.subscriber(*args2, **kwargs2)
        def subscriber(m) -> None:
            mock()
            if not event.is_set():
                event.set()
            else:
                event2.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(br.publish("hello", queue + "1")),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2

    async def test_consume_double(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(m) -> None:
            mock()
            if not event.is_set():
                event.set()
            else:
                event2.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event2.is_set()
        assert event.is_set()
        assert mock.call_count == 2

    async def test_different_consume(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @consume_broker.subscriber(*args, **kwargs)
        def handler(m) -> None:
            mock.handler()
            event.set()

        another_topic = queue + "1"
        args, kwargs = self.get_subscriber_params(another_topic)

        @consume_broker.subscriber(*args, **kwargs)
        def handler2(m) -> None:
            mock.handler2()
            event2.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(br.publish("hello", another_topic)),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert event2.is_set()
        mock.handler.assert_called_once()
        mock.handler2.assert_called_once()

    async def test_consume_with_filter(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            queue,
        )

        sub = consume_broker.subscriber(*args, **kwargs)

        @sub(filter=lambda m: m.content_type == "application/json")
        async def handler(m) -> None:
            mock.handler(m)
            event.set()

        @sub
        async def handler2(m) -> None:
            mock.handler2(m)
            event2.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish({"msg": "hello"}, queue)),
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert event2.is_set()
        mock.handler.assert_called_once_with({"msg": "hello"})
        mock.handler2.assert_called_once_with("hello")

    async def test_consume_validate_false(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        consume_broker = self.get_broker(
            apply_types=True,
            serializer=None,
        )

        class Foo(BaseModel):
            x: int

        def dependency() -> str:
            return "100"

        args, kwargs = self.get_subscriber_params(queue)

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(
            m: Foo,
            dep: int = Depends(dependency),
            broker=Context(),
        ) -> None:
            mock(m, dep, broker)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish({"x": 1}, queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            mock.assert_called_once_with({"x": 1}, "100", consume_broker)

    async def test_dynamic_sub(self, queue: str, event: asyncio.Event) -> None:
        consume_broker = self.get_broker()

        async def subscriber(m) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            args, kwargs = self.get_subscriber_params(queue)
            sub = br.subscriber(*args, **kwargs)
            sub(subscriber)
            await sub.start()

            await br.publish("hello", queue)

            with anyio.move_on_after(self.timeout):
                await event.wait()

            await sub.stop()

        assert event.is_set()

    async def test_sub_start_use_context_manager(
        self, queue: str, event: asyncio.Event
    ) -> None:
        consume_broker = self.get_broker()

        async def subscriber(m) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            args, kwargs = self.get_subscriber_params(queue)
            sub = br.subscriber(*args, **kwargs)
            sub(subscriber)

            async with sub:
                await br.publish("hello", queue)

                with anyio.move_on_after(self.timeout):
                    await event.wait()

        assert event.is_set()

    async def test_get_one_conflicts_with_handler(self, queue) -> None:
        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def t() -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()

            with pytest.raises(AssertionError):
                await subscriber.get_one(timeout=1e-24)

    async def test_composition_context(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(
            apply_types=True,
            context=ContextRepo({"broker_context": "broker_context"}),
        )

        app = FastStream(broker, context=ContextRepo({"app_context": "app_context"}))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(app_context=Context(), broker_context=Context()) -> None:
            mock(app_context, broker_context)
            event.set()

        async with self.patch_broker(broker) as br, TestApp(app):
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            mock.assert_called_once_with("app_context", "broker_context")

    async def test_composition_context_merge(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(
            apply_types=True,
            context=ContextRepo({"context_var": "BROKER"}),
        )

        app = FastStream(broker, context=ContextRepo({"context_var": "APP"}))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(context_var=Context()) -> None:
            mock(context_var)
            event.set()

        async with self.patch_broker(broker) as br, TestApp(app):
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            mock.assert_called_once_with("BROKER")

    async def test_multi_broker_composition_context(
        self,
        queue: str,
        mock: MagicMock,
        mock2: MagicMock,
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        broker = self.get_broker(
            apply_types=True,
            context=ContextRepo({"broker_var": "BROKER 1"}),
        )
        broker2 = self.get_broker(
            apply_types=True,
            context=ContextRepo({"broker_var": "BROKER 2"}),
        )

        app = FastStream(broker, broker2, context=ContextRepo({"app_var": "APP"}))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(app_var=Context(), broker_var=Context()) -> None:
            event.set()
            mock(app_var, broker_var)

        args2, kwargs2 = self.get_subscriber_params(queue + "2")

        @broker2.subscriber(*args2, **kwargs2)
        async def handle2(app_var=Context(), broker_var=Context()) -> None:
            event2.set()
            mock2(app_var, broker_var)

        async with (
            self.patch_broker(broker) as br,
            self.patch_broker(broker2) as br2,
            TestApp(app),
        ):
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(br2.publish("", queue + "2")),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert event2.is_set()
        mock.assert_called_once_with("APP", "BROKER 1")
        mock2.assert_called_once_with("APP", "BROKER 2")


@pytest.mark.asyncio()
class BrokerRealConsumeTestcase(BrokerConsumeTestcase):
    async def test_get_one(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        async with self.patch_broker(broker) as br:
            await br.start()

            async def consume() -> None:
                mock(await subscriber.get_one(timeout=self.timeout))

            async def publish() -> None:
                await anyio.sleep(1e-24)
                await br.publish("test_message", queue)

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish()),
                ),
                timeout=self.timeout,
            )

            mock.assert_called_once()
            message = mock.call_args[0][0]
            assert message
            assert await message.decode() == "test_message", await message.decode()

    async def test_get_one_timeout(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)

    @pytest.mark.slow()
    async def test_stop_consume_exc(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @consume_broker.subscriber(*args, **kwargs)
        def subscriber(m):
            mock()
            event.set()
            raise StopConsume

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )
            await asyncio.sleep(0.5)
            await br.publish("hello", queue)
            await asyncio.sleep(0.5)

        assert event.is_set()
        mock.assert_called_once()

    @pytest.mark.asyncio()
    async def test_iteration(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        expected_messages = ("test_message_1", "test_message_2")

        broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        async with self.patch_broker(broker) as br:
            await br.start()

            async def publish_test_message():
                for msg in expected_messages:
                    await br.publish(msg, queue)

            async def consume():
                index_message = 0
                async for msg in subscriber:
                    result_message = await msg.decode()

                    mock(result_message)

                    index_message += 1
                    if index_message >= len(expected_messages):
                        break

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish_test_message()),
                ),
                timeout=self.timeout,
            )

            calls = [call(msg) for msg in expected_messages]
            mock.assert_has_calls(calls=calls)
