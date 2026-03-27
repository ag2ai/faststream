import asyncio

import anyio
import pytest

from faststream.mq.broker.router import MQPublisher, MQRoute
from tests.brokers.base.router import RouterTestcase
from tests.marks import require_ibmmq

from .basic import MQMemoryTestcaseConfig, MQTestcaseConfig


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestRouter(MQMemoryTestcaseConfig, RouterTestcase):
    route_class = MQRoute
    publisher_class = MQPublisher

    @require_ibmmq
    @pytest.mark.connected()
    async def test_iterator_respect_decoder(self, mock, queue):
        start_event, consumed_event, stopped_event = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )

        broker = MQTestcaseConfig.get_broker(self)

        async def custom_decoder(msg, original):
            mock()
            consumed_event.set()
            return await original(msg)

        args, kwargs = self.get_subscriber_params(queue, decoder=custom_decoder)
        sub = broker.subscriber(*args, **kwargs)

        async def iter_messages() -> bytes:
            await sub.start()
            start_event.set()

            async for m in sub:
                assert not mock.called
                data = await m.decode()
                mock.assert_called_once()
                break

            await sub.stop()
            stopped_event.set()
            return data

        async with broker:
            task = asyncio.create_task(iter_messages())

            with anyio.move_on_after(self.timeout):
                await start_event.wait()
                await broker.publish(b"hello", queue)
                await consumed_event.wait()
                await stopped_event.wait()

            await task
            assert task.result() == b"hello"

    @require_ibmmq
    @pytest.mark.connected()
    async def test_get_one_respect_decoder(self, queue, event, mock):
        broker = MQTestcaseConfig.get_broker(self)

        async def custom_decoder(msg, original):
            mock()
            return await original(msg)

        args, kwargs = self.get_subscriber_params(queue, decoder=custom_decoder)
        sub = broker.subscriber(*args, **kwargs)

        async def get_msg():
            await sub.start()
            event.set()
            msg = await sub.get_one(timeout=self.timeout)
            await sub.stop()
            return msg

        async with broker:
            task = asyncio.create_task(get_msg())

            with anyio.move_on_after(self.timeout):
                await event.wait()
                await broker.publish(b"hello", queue)

            await task
            msg = task.result()

        assert not mock.called
        assert msg is not None
        await msg.decode()
        mock.assert_called_once()

    async def test_include_publisher_with_prefix(self, queue: str, event) -> None:
        broker = self.get_broker()

        args2, kwargs2 = self.get_subscriber_params(f"test_{queue}")

        @broker.subscriber(*args2, **kwargs2)
        async def handler(m) -> None:
            event.set()

        router = self.get_router()
        publisher = router.publisher(queue)
        broker.include_router(router, prefix="test_")

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(publisher.publish("hello")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
