import asyncio
from abc import abstractmethod
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest
from pydantic import BaseModel

from faststream import Context, Depends
from faststream.broker.core.usecase import BrokerUsecase
from faststream.exceptions import StopConsume

from .basic import BaseTestcaseConfig


@pytest.mark.asyncio
class BrokerConsumeTestcase(BaseTestcaseConfig):
    @abstractmethod
    def get_broker(self, broker: BrokerUsecase) -> BrokerUsecase[Any, Any]:
        raise NotImplementedError

    def patch_broker(self, broker: BrokerUsecase[Any, Any]) -> BrokerUsecase[Any, Any]:
        return broker

    async def test_consume(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @consume_broker.subscriber(*args, **kwargs)
        def subscriber(m):
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
    ):
        consume_broker = self.get_broker()

        consume = asyncio.Event()
        consume2 = asyncio.Event()

        args, kwargs = self.get_subscriber_params(queue)
        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @consume_broker.subscriber(*args, **kwargs)
        @consume_broker.subscriber(*args2, **kwargs2)
        def subscriber(m):
            mock()
            if not consume.is_set():
                consume.set()
            else:
                consume2.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(br.publish("hello", queue + "1")),
                    asyncio.create_task(consume.wait()),
                    asyncio.create_task(consume2.wait()),
                ),
                timeout=self.timeout,
            )

        assert consume2.is_set()
        assert consume.is_set()
        assert mock.call_count == 2

    async def test_consume_double(
        self,
        queue: str,
        mock: MagicMock,
    ):
        consume_broker = self.get_broker()

        consume = asyncio.Event()
        consume2 = asyncio.Event()

        args, kwargs = self.get_subscriber_params(queue)

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(m):
            mock()
            if not consume.is_set():
                consume.set()
            else:
                consume2.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(consume.wait()),
                    asyncio.create_task(consume2.wait()),
                ),
                timeout=self.timeout,
            )

        assert consume2.is_set()
        assert consume.is_set()
        assert mock.call_count == 2

    async def test_different_consume(
        self,
        queue: str,
        mock: MagicMock,
    ):
        consume_broker = self.get_broker()

        consume = asyncio.Event()
        consume2 = asyncio.Event()

        args, kwargs = self.get_subscriber_params(queue)

        @consume_broker.subscriber(*args, **kwargs)
        def handler(m):
            mock.handler()
            consume.set()

        another_topic = queue + "1"
        args, kwargs = self.get_subscriber_params(another_topic)

        @consume_broker.subscriber(*args, **kwargs)
        def handler2(m):
            mock.handler2()
            consume2.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(br.publish("hello", another_topic)),
                    asyncio.create_task(consume.wait()),
                    asyncio.create_task(consume2.wait()),
                ),
                timeout=self.timeout,
            )

        assert consume.is_set()
        assert consume2.is_set()
        mock.handler.assert_called_once()
        mock.handler2.assert_called_once()

    async def test_consume_with_filter(
        self,
        queue: str,
        mock: MagicMock,
    ):
        consume_broker = self.get_broker()

        consume = asyncio.Event()
        consume2 = asyncio.Event()

        args, kwargs = self.get_subscriber_params(
            queue,
        )

        sub = consume_broker.subscriber(*args, **kwargs)

        @sub(filter=lambda m: m.content_type == "application/json")
        async def handler(m):
            mock.handler(m)
            consume.set()

        @sub
        async def handler2(m):
            mock.handler2(m)
            consume2.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish({"msg": "hello"}, queue)),
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(consume.wait()),
                    asyncio.create_task(consume2.wait()),
                ),
                timeout=self.timeout,
            )

        assert consume.is_set()
        assert consume2.is_set()
        mock.handler.assert_called_once_with({"msg": "hello"})
        mock.handler2.assert_called_once_with("hello")

    async def test_consume_validate_false(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ):
        consume_broker = self.get_broker()

        consume_broker._is_apply_types = True
        consume_broker._is_validate = False

        class Foo(BaseModel):
            x: int

        def dependency() -> str:
            return "100"

        args, kwargs = self.get_subscriber_params(queue)

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(m: Foo, dep: int = Depends(dependency), broker=Context()):
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

    async def test_dynamic_sub(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        consume_broker = self.get_broker()

        async def subscriber(m):
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            args, kwargs = self.get_subscriber_params(queue)
            sub = br.subscriber(*args, **kwargs)
            sub(subscriber)
            br.setup_subscriber(sub)
            await sub.start()

            await br.publish("hello", queue)

            with anyio.move_on_after(self.timeout):
                await event.wait()

            await sub.stop()

        assert event.is_set()

    async def test_get_one_conflicts_with_handler(self, queue):
        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def t(): ...

        async with self.patch_broker(broker) as br:
            await br.start()

            with pytest.raises(AssertionError):
                await subscriber.get_one(timeout=1e-24)


@pytest.mark.asyncio
class BrokerRealConsumeTestcase(BrokerConsumeTestcase):
    async def test_get_one(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ):
        broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        async with self.patch_broker(broker) as br:
            await br.start()

            async def consume():
                mock(await subscriber.get_one(timeout=self.timeout))

            async def publish():
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
            assert await message.decode() == "test_message"

    async def test_get_one_timeout(
        self,
        queue: str,
        mock: MagicMock,
    ):
        broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        async with self.patch_broker(broker) as br:
            await br.start()

            mock(await subscriber.get_one(timeout=1e-24))
            mock.assert_called_once_with(None)

    @pytest.mark.slow
    async def test_stop_consume_exc(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ):
        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @consume_broker.subscriber(*args, **kwargs)
        def subscriber(m):
            mock()
            event.set()
            raise StopConsume()

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
