import asyncio
from collections.abc import Sequence
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from faststream import AckPolicy, BaseMiddleware, Context
from faststream._internal.parser import DefaultCodec
from faststream.confluent import KafkaResponse
from faststream.confluent.annotations import KafkaMessage
from faststream.confluent.message import FAKE_CONSUMER
from faststream.confluent.testing import FakeProducer
from faststream.message import TOMBSTONE, Tombstone
from tests.brokers.base.testclient import BrokerTestclientTestcase
from tests.tools import spy_decorator

from .basic import ConfluentMemoryTestcaseConfig


class _BatchCodec(DefaultCodec):
    async def encode_batch(
        self,
        msgs: Sequence[Any],
        serializer: Any = None,
    ) -> list[tuple[bytes, str | None]]:
        return [await DefaultCodec.encode(self, m, serializer) for m in msgs]

    async def decode_batch(self, msg: Any) -> list[Any]:
        return list(msg.body)


@pytest.mark.confluent()
@pytest.mark.asyncio()
class TestTestclient(ConfluentMemoryTestcaseConfig, BrokerTestclientTestcase):
    async def test_message_nack_seek(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(
            queue,
            group_id=f"{queue}-consume",
            auto_offset_reset="earliest",
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def m(msg: KafkaMessage) -> None:
            await msg.nack()

        async with self.patch_broker(broker) as br:
            with patch.object(
                FAKE_CONSUMER,
                "seek",
                spy_decorator(FAKE_CONSUMER.seek),
            ) as mocked:
                await br.publish("hello", queue)
                m.mock.assert_called_once_with("hello")
                mocked.mock.assert_called_once()

    @pytest.mark.connected()
    async def test_with_real_testclient(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        def subscriber(m) -> None:
            event.set()

        async with self.patch_broker(broker, with_real=True) as br:
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=10,
            )

        assert event.is_set()

    async def test_publisher_autoflush_mock(self, queue: str) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue + "1", autoflush=True)
        publisher.flush = AsyncMock()

        @publisher
        @broker.subscriber(queue)
        async def m(msg):
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)

            m.mock.assert_called_once_with("hello")
            publisher.mock.assert_called_once()

            publisher.flush.assert_awaited_once()

    async def test_batch_publisher_autoflush_mock(self, queue: str) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue + "1", batch=True, autoflush=True)
        publisher.flush = AsyncMock()

        @publisher
        @broker.subscriber(queue)
        async def m(msg):
            return 1, 2, 3

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)

            m.mock.assert_called_once_with("hello")
            publisher.mock.assert_called_once_with([1, 2, 3])

            publisher.flush.assert_awaited_once()

    async def test_batch_pub_by_default_pub(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, batch=True)
        async def m(msg) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            m.mock.assert_called_once_with(["hello"])

    async def test_batch_pub_by_pub_batch(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, batch=True)
        async def m(msg) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish_batch("hello", topic=queue)
            m.mock.assert_called_once_with(["hello"])

    async def test_batch_publisher_mock(self, queue: str) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue + "1", batch=True)

        @publisher
        @broker.subscriber(queue)
        async def m(msg):
            return 1, 2, 3

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            m.mock.assert_called_once_with("hello")
            publisher.mock.assert_called_once_with([1, 2, 3])

    async def test_respect_middleware(self, queue: str) -> None:
        routes = []

        class Middleware(BaseMiddleware):
            async def on_receive(self) -> None:
                routes.append(None)
                return await super().on_receive()

        broker = self.get_broker(middlewares=(Middleware,))

        @broker.subscriber(queue)
        async def h1(msg) -> None: ...

        @broker.subscriber(queue + "1")
        async def h2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("", queue)
            await br.publish("", queue + "1")

        assert len(routes) == 2

    @pytest.mark.connected()
    async def test_real_respect_middleware(self, queue: str) -> None:
        routes = []

        class Middleware(BaseMiddleware):
            async def on_receive(self) -> None:
                routes.append(None)
                return await super().on_receive()

        broker = self.get_broker(middlewares=(Middleware,))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def h1(msg) -> None: ...

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @broker.subscriber(*args2, **kwargs2)
        async def h2(msg) -> None: ...

        async with self.patch_broker(broker, with_real=True) as br:
            await br.publish("", queue)
            await br.publish("", queue + "1")
            await h1.wait_call(10)
            await h2.wait_call(10)

        assert len(routes) == 2

    async def test_multiple_subscribers_different_groups(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, group_id="group1")
        async def subscriber1(msg) -> None: ...

        @broker.subscriber(queue, group_id="group2")
        async def subscriber2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("", queue)

            assert subscriber1.mock.call_count == 1
            assert subscriber2.mock.call_count == 1

    async def test_multiple_subscribers_same_group(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, group_id="group1")
        async def subscriber1(msg) -> None: ...

        @broker.subscriber(queue, group_id="group1")
        async def subscriber2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("", queue)

            # we can't guarantee the order of calls
            assert {subscriber1.mock.call_count, subscriber2.mock.call_count} == {1, 0}

    async def test_multiple_batch_subscriber_with_different_group(
        self, queue: str
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, batch=True, group_id="group1")
        async def subscriber1(msg) -> None: ...

        @broker.subscriber(queue, batch=True, group_id="group2")
        async def subscriber2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("", queue)

            assert subscriber1.mock.call_count == 1
            assert subscriber2.mock.call_count == 1

    async def test_multiple_batch_subscriber_with_same_group(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, batch=True, group_id="group1")
        async def subscriber1(msg) -> None: ...

        @broker.subscriber(queue, batch=True, group_id="group1")
        async def subscriber2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("", queue)

            # we can't guarantee the order of calls
            assert {subscriber1.mock.call_count, subscriber2.mock.call_count} == {1, 0}

    @pytest.mark.connected()
    async def test_broker_gets_patched_attrs_within_cm(self) -> None:
        await super().test_broker_gets_patched_attrs_within_cm(FakeProducer)

    @pytest.mark.connected()
    async def test_broker_with_real_doesnt_get_patched(self) -> None:
        await super().test_broker_with_real_doesnt_get_patched()

    @pytest.mark.connected()
    async def test_broker_with_real_patches_publishers_and_subscribers(
        self, queue: str
    ) -> None:
        await super().test_broker_with_real_patches_publishers_and_subscribers(queue)

    @pytest.mark.xfail(reason="https://github.com/ag2ai/faststream/issues/2513")
    async def test_publisher_without_destination(self) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2513."""
        broker = self.get_broker()

        # use two publishers to check that we don't have conflicts
        publisher = broker.publisher(topic="")
        another_publisher = broker.publisher(topic="")

        async with self.patch_broker(broker):
            await publisher.publish(None, topic="new-key")
            publisher.mock.assert_called_once()

            await another_publisher.publish(None, topic="new-key")
            another_publisher.mock.assert_called_once()

    async def test_publish_none_still_tombstones_but_warns(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg=Context("message")) -> None:
            await values.put(msg.raw_message.value())

        async with self.patch_broker(broker) as br:
            with pytest.deprecated_call(match="tombstone"):
                await br.publish(None, queue, key=b"legacy-key")
            value = await asyncio.wait_for(values.get(), timeout=3)

        assert value is None

    async def test_a_tombstone_body_still_reads_as_empty_bytes(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        bodies: asyncio.Queue[bytes] = asyncio.Queue()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: bytes) -> None:
            await bodies.put(msg)

        async with self.patch_broker(broker) as br:
            await br.publish(TOMBSTONE, queue, key=b"tombstone-key")
            body = await asyncio.wait_for(bodies.get(), timeout=3)

        assert body == b""
        assert isinstance(body, Tombstone)

    async def test_publish_tombstone_sends_a_real_tombstone(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg=Context("message")) -> None:
            await values.put(msg.raw_message.value())

        async with self.patch_broker(broker) as br:
            await br.publish(TOMBSTONE, queue, key=b"tombstone-key")
            value = await asyncio.wait_for(values.get(), timeout=3)

        assert value is None

    async def test_publish_tombstone_without_key_raises(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            with pytest.raises(ValueError, match="requires a key"):
                await br.publish(TOMBSTONE, queue)

    async def test_batch_tombstone_with_custom_batch_codec_raises(
        self, queue: str
    ) -> None:
        broker = self.get_broker(codec=_BatchCodec())

        @broker.subscriber(queue, batch=True)
        async def handler(msg: list[bytes]) -> None: ...

        async with self.patch_broker(broker) as br:
            with pytest.raises(ValueError, match="BatchCodecProto"):
                await br.publish_batch(
                    b"hi",
                    KafkaResponse(TOMBSTONE, key=b"k"),
                    topic=queue,
                )

    async def test_plain_none_in_a_batch_is_not_a_tombstone(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        values: list[object] = []

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: list[bytes]) -> None:
            values.extend(msg)

        async with self.patch_broker(broker) as br:
            await br.publish_batch(b"hi", None, topic=queue)

        assert not any(isinstance(v, Tombstone) for v in values)

    async def test_publish_batch_with_tombstone(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        values: list[bytes | None] = []

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: list[bytes | None]) -> None:
            values.extend(msg)

        async with self.patch_broker(broker) as br:
            await br.publish_batch(
                b"hi",
                KafkaResponse(TOMBSTONE, key=b"batch-tombstone-key"),
                topic=queue,
            )

        assert b"hi" in values
        assert any(isinstance(v, Tombstone) for v in values)
