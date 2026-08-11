import asyncio
from unittest.mock import MagicMock, call, patch

import anyio
import pytest
from pydantic import BaseModel

from faststream import AckPolicy, Context, Depends, FastStream, TestApp
from faststream.exceptions import StopConsume
from faststream.message import StreamMessage
from tests.tools import spy_decorator

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

    @pytest.mark.parametrize(
        "ack_policy",
        (
            pytest.param(AckPolicy.ACK, id="ack"),
            pytest.param(
                AckPolicy.REJECT_ON_ERROR,
                id="reject_on_error",
                marks=[
                    pytest.mark.filterwarnings(
                        "ignore:AckPolicy.REJECT_ON_ERROR has the same effect"
                    )
                ],
            ),
            pytest.param(AckPolicy.NACK_ON_ERROR, id="nack_on_error"),
        ),
    )
    async def test_consume_cancel_skips_ack_nack_reject(
        self,
        queue: str,
        ack_policy: AckPolicy,
    ) -> None:
        if not self.supports_cancel_ack_skip:
            pytest.skip("broker default subscriber has no acknowledgement middleware")

        started = asyncio.Event()
        broker = self.get_broker(graceful_timeout=0.2)
        args, kwargs = self.get_subscriber_params(
            queue,
            ack_policy=ack_policy,
            **self.get_cancel_ack_subscriber_kwargs(queue),
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(_msg: object) -> None:
            started.set()
            await asyncio.sleep(60)

        with (
            patch.object(
                StreamMessage,
                "nack",
                spy_decorator(StreamMessage.nack),
            ) as nack,
            patch.object(
                StreamMessage,
                "ack",
                spy_decorator(StreamMessage.ack),
            ) as ack,
            patch.object(
                StreamMessage,
                "reject",
                spy_decorator(StreamMessage.reject),
            ) as reject,
        ):
            async with self.patch_broker(broker) as br:
                await br.start()
                publish_task = asyncio.create_task(br.publish("hello", queue))
                await asyncio.wait_for(started.wait(), timeout=self.timeout)

                # TestBroker awaits the handler inside publish; real brokers
                # return after enqueue, so cancel publish when still running,
                # otherwise stop and let graceful_timeout cancel the consume task.
                if not publish_task.done():
                    publish_task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await publish_task
                else:
                    await publish_task
                    await br.stop()

        nack.mock.assert_not_awaited()
        ack.mock.assert_not_awaited()
        reject.mock.assert_not_awaited()


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
