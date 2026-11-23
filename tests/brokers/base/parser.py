import asyncio
from abc import abstractmethod
from contextlib import aclosing
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest

from faststream import Context

from .basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class LocalCustomParserTestcase(BaseTestcaseConfig[Any]):
    async def test_local_parser(
        self,
        mock: MagicMock,
        event: asyncio.Event,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        async def custom_parser(msg: Any, original: Any) -> Any:
            msg = await original(msg)
            mock(msg.body)
            return msg

        args, kwargs = self.get_subscriber_params(queue, parser=custom_parser)

        @broker.subscriber(*args, **kwargs)
        async def handle(m: Any) -> None:
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

        def custom_decoder(msg: Any) -> Any:
            mock(msg.body)
            return msg

        args, kwargs = self.get_subscriber_params(queue, decoder=custom_decoder)

        @broker.subscriber(*args, **kwargs)
        async def handle(m: Any) -> None:
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
        def custom_decoder(msg: Any) -> Any:
            mock(msg.body)
            return msg

        broker = self.get_broker(decoder=custom_decoder)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(m: Any) -> None:
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
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        broker = self.get_broker()

        async def custom_parser(msg: Any, original: Any) -> Any:
            msg = await original(msg)
            mock(msg.body)
            return msg

        args, kwargs = self.get_subscriber_params(queue, parser=custom_parser)
        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @broker.subscriber(*args, **kwargs)
        @broker.subscriber(*args2, **kwargs2)
        async def handle(m: Any) -> None:
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
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)
        sub = broker.subscriber(*args, **kwargs)

        @sub(filter=lambda m: m.content_type == "application/json")
        async def handle(m: Any) -> None:
            event.set()

        async def custom_parser(msg: Any, original: Any) -> Any:
            msg = await original(msg)
            mock(msg.body)
            return msg

        @sub(parser=custom_parser)
        async def handle2(m: Any) -> None:
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
        event: asyncio.Event,
        event2: asyncio.Event,
        event3: asyncio.Event,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2554."""
        start_event, consumed_event, stopped_event = event, event2, event3

        broker = self.get_broker()

        async def custom_decoder(msg: Any, original: Any) -> Any:
            mock()
            consumed_event.set()
            return await original(msg)

        args, kwargs = self.get_subscriber_params(queue, decoder=custom_decoder)
        sub = broker.subscriber(*args, **kwargs)

        async def iter_messages() -> Any:
            await sub.start()
            start_event.set()

            msg: Any = None
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

        async def custom_decoder(msg: Any, original: Any) -> Any:
            mock()
            return await original(msg)

        args, kwargs = self.get_subscriber_params(queue, decoder=custom_decoder)
        sub = broker.subscriber(*args, **kwargs)

        async def get_msg() -> Any:
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
        async def custom_parser(msg: Any, original: Any) -> Any:
            msg = await original(msg)
            mock(msg.body)
            return msg

        broker = self.get_broker(parser=custom_parser)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(m: Any) -> None:
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


@pytest.mark.asyncio()
class KafkaTombstoneConsumeTestcase(BaseTestcaseConfig[Any]):
    @abstractmethod
    async def publish_native_records(
        self,
        url: str,
        topic: str,
        records: tuple[tuple[bytes, bytes | None, str], ...],
    ) -> None:
        raise NotImplementedError

    @pytest.mark.parametrize(
        "delivery",
        (
            pytest.param("single", id="single"),
            pytest.param("batch", id="batch"),
            pytest.param("poll", id="get-one"),
            pytest.param("iterate", id="iterator"),
        ),
    )
    async def test_decodes_and_marks_native_tombstones(
        self, queue: str, settings: Any, delivery: str
    ) -> None:
        batch = delivery == "batch"
        broker = self.get_broker(apply_types=True)
        received: asyncio.Queue[tuple[Any, bool, bytes, bytes | None, int]] = (
            asyncio.Queue()
        )
        records = (
            (b"object", b'{"x": 1}', "application/json"),
            (b"delete", None, "application/json"),
            (b"empty", b"", ""),
            (b"null", b"null", "application/json"),
        )
        args, kwargs = self.get_subscriber_params(queue, batch=batch, group_id=queue)

        subscriber = broker.subscriber(*args, **kwargs)

        async def handler(message: Any = Context("message")) -> None:
            decoded = await message.decode()
            items = (
                zip(decoded, message.batch_tombstones, message.raw_message, strict=True)
                if batch
                else ((decoded, message.tombstone, message.raw_message),)
            )
            for body, tombstone, raw in items:
                wire = (
                    (raw.key(), raw.value(), raw.partition())
                    if callable(raw.key)
                    else (raw.key, raw.value, raw.partition)
                )
                received.put_nowait((body, tombstone, *wire))

        if delivery in {"single", "batch"}:
            subscriber(handler)

        async with self.patch_broker(broker) as br:
            await self.publish_native_records(settings.url, queue, records)
            await br.start()
            if delivery == "poll":
                for _ in records:
                    message = await subscriber.get_one(timeout=self.timeout)
                    assert message is not None
                    await handler(message)
            elif delivery == "iterate":
                async with aclosing(aiter(subscriber)) as messages:
                    for _ in records:
                        await handler(
                            await asyncio.wait_for(anext(messages), self.timeout)
                        )
            actual = [
                await asyncio.wait_for(received.get(), self.timeout) for _ in records
            ]

        assert actual == [
            ({"x": 1}, False, b"object", b'{"x": 1}', 0),
            (b"", True, b"delete", None, 0),
            (b"", False, b"empty", b"", 0),
            (None, False, b"null", b"null", 0),
        ]
