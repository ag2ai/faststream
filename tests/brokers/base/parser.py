import asyncio
from unittest.mock import MagicMock

import anyio
import pytest

from faststream.message.message import StreamMessage

from .basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class LocalCustomParserTestcase(BaseTestcaseConfig):
    async def test_local_parser(
        self,
        mock: MagicMock,
        event: asyncio.Event,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        async def custom_parser(msg, original):
            msg = await original(msg)
            mock(msg.body)
            return msg

        args, kwargs = self.get_subscriber_params(queue, parser=custom_parser)

        @broker.subscriber(*args, **kwargs)
        async def handle(m) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(b"hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            mock.assert_called_once_with(b"hello")

    async def test_local_sync_decoder(
        self,
        mock: MagicMock,
        event: asyncio.Event,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        def custom_decoder(msg):
            mock(msg.body)
            return msg

        args, kwargs = self.get_subscriber_params(queue, decoder=custom_decoder)

        @broker.subscriber(*args, **kwargs)
        async def handle(m) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(b"hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            mock.assert_called_once_with(b"hello")

    async def test_global_sync_decoder(
        self,
        mock: MagicMock,
        event: asyncio.Event,
        queue: str,
    ) -> None:
        def custom_decoder(msg):
            mock(msg.body)
            return msg

        broker = self.get_broker(decoder=custom_decoder)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(m) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(b"hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            mock.assert_called_once_with(b"hello")

    async def test_local_parser_no_share_between_subscribers(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        event, event2 = asyncio.Event(), asyncio.Event()
        broker = self.get_broker()

        async def custom_parser(msg, original):
            msg = await original(msg)
            mock(msg.body)
            return msg

        args, kwargs = self.get_subscriber_params(queue, parser=custom_parser)
        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @broker.subscriber(*args, **kwargs)
        @broker.subscriber(*args2, **kwargs2)
        async def handle(m) -> None:
            if event.is_set():
                event2.set()
            else:
                event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(b"hello", queue)),
                    asyncio.create_task(br.publish(b"hello", queue + "1")),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            assert event2.is_set()
            mock.assert_called_once_with(b"hello")

    async def test_local_parser_no_share_between_handlers(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        event, event2 = asyncio.Event(), asyncio.Event()

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)
        sub = broker.subscriber(*args, **kwargs)

        @sub(filter=lambda m: m.content_type == "application/json")
        async def handle(m) -> None:
            event.set()

        async def custom_parser(msg, original):
            msg = await original(msg)
            mock(msg.body)
            return msg

        @sub(parser=custom_parser)
        async def handle2(m) -> None:
            event2.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish({"msg": "hello"}, queue)),
                    asyncio.create_task(br.publish(b"hello", queue)),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            assert event2.is_set()
            assert mock.call_count == 1

    @pytest.mark.connected()
    async def test_iterator_respect_decoder(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2554."""
        start_event, consumed_event, stopped_event = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )

        broker = self.get_broker()

        async def custom_decoder(msg, original):
            mock()
            consumed_event.set()
            return await original(msg)

        args, kwargs = self.get_subscriber_params(queue, decoder=custom_decoder)
        sub = broker.subscriber(*args, **kwargs)

        async def iter_messages() -> None:
            await sub.start()
            start_event.set()

            async for m in sub:
                assert not mock.called
                msg = await m.decode()
                mock.assert_called_once()
                break

            await sub.stop()
            stopped_event.set()
            return msg

        async with broker:
            t = asyncio.create_task(iter_messages())

            with anyio.move_on_after(self.timeout):
                await start_event.wait()
                await broker.publish(b"hello", queue)
                await consumed_event.wait()
                await stopped_event.wait()

            await t
            data = t.result()
            assert data == b"hello", data

    @pytest.mark.connected()
    async def test_get_one_respect_decoder(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2554."""
        broker = self.get_broker()

        async def custom_decoder(msg, original):
            mock()
            return await original(msg)

        args, kwargs = self.get_subscriber_params(queue, decoder=custom_decoder)
        sub = broker.subscriber(*args, **kwargs)

        async def get_msg() -> StreamMessage:
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

        assert not mock.called, mock.call_count
        await msg.decode()
        mock.assert_called_once()


class CustomParserTestcase(LocalCustomParserTestcase):
    async def test_global_parser(
        self,
        mock: MagicMock,
        event: asyncio.Event,
        queue: str,
    ) -> None:
        async def custom_parser(msg, original):
            msg = await original(msg)
            mock(msg.body)
            return msg

        broker = self.get_broker(parser=custom_parser)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(m) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(b"hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

            assert event.is_set()
            mock.assert_called_once_with(b"hello")
