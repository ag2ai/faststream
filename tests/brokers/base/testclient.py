import asyncio
import gc
import json
from abc import abstractmethod
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import anyio
import pytest
from dirty_equals import IsPartialDict
from pydantic import BaseModel

from faststream import Context
from faststream._internal.kafka import TOMBSTONE, Tombstone
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import SetupError
from tests.tools import spy_decorator

from .basic import BaseTestcaseConfig
from .consume import BrokerConsumeTestcase
from .publish import BrokerPublishTestcase

if TYPE_CHECKING:
    from collections.abc import Sequence


class BodyModel(BaseModel):
    name: str
    age: int


class BrokerTestclientTestcase(BrokerPublishTestcase, BrokerConsumeTestcase):
    @abstractmethod
    def get_fake_producer_class(self) -> type:
        raise NotImplementedError

    @pytest.mark.asyncio()
    async def test_correct_clean_fake_subscribers(self) -> None:
        broker = self.get_broker()

        @broker.subscriber("test")
        async def handler1(msg) -> None: ...

        # protect publishers from gc
        pub1 = broker.publisher("test2")  # noqa: F841
        pub2 = broker.publisher("test")  # noqa: F841

        assert len(broker.subscribers) == 1, len(broker.subscribers)

        test_client = self.patch_broker(broker)
        async with test_client as br:
            assert len(br.subscribers) == 2, len(broker.subscribers)

        gc.collect()
        assert len(broker.subscribers) == 1, len(broker.subscribers)

    @pytest.mark.asyncio()
    async def test_fake_subscribers_deregistered_without_gc(self) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2990.

        A second TestBroker must not reuse a fake left behind by the first.
        """
        broker = self.get_broker()

        @broker.subscriber("test")
        async def handler(msg) -> None: ...

        pub = broker.publisher("test2")  # noqa: F841

        async with self.patch_broker(broker):
            pass

        # No gc.collect() before this line: the leftover fake stayed weakly reachable
        # until the next collection, which is exactly what hid the bug.
        assert len(broker.subscribers) == 1, len(broker.subscribers)

        second_client = self.patch_broker(broker)
        async with second_client as br:
            # This client owns its own fake, so the collector cannot take it away
            # mid-test and leave `publish()` raising `SubscriberNotFound`.
            assert len(second_client._fake_subscribers) == 1
            gc.collect()
            assert len(br.subscribers) == 2, len(br.subscribers)

    @pytest.mark.asyncio()
    async def test_subscriber_mock(self, queue: str) -> None:
        test_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @test_broker.subscriber(*args, **kwargs)
        async def m(msg) -> None:
            pass

        async with self.patch_broker(test_broker) as br:
            await br.start()
            await br.publish("hello", queue)
            m.mock.assert_called_once_with("hello")

    @pytest.mark.asyncio()
    async def test_publisher_mock(self, queue: str) -> None:
        test_broker = self.get_broker()

        publisher = test_broker.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @publisher
        @test_broker.subscriber(*args, **kwargs)
        async def m(msg) -> str:
            return "response"

        async with self.patch_broker(test_broker) as br:
            await br.start()
            await br.publish("hello", queue)
            publisher.mock.assert_called_with("response")

    @pytest.mark.asyncio()
    async def test_publisher_with_subscriber__mock(self, queue: str) -> None:
        test_broker = self.get_broker()

        publisher = test_broker.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @publisher
        @test_broker.subscriber(*args, **kwargs)
        async def m(msg) -> str:
            return "response"

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @test_broker.subscriber(*args2, **kwargs2)
        async def handler_response(msg) -> None: ...

        async with self.patch_broker(test_broker) as br:
            await br.start()

            assert len(br.subscribers) == 2

            await br.publish("hello", queue)
            publisher.mock.assert_called_with("response")
            handler_response.mock.assert_called_once_with("response")

    @pytest.mark.asyncio()
    async def test_manual_publisher_mock(self, queue: str) -> None:
        test_broker = self.get_broker()

        publisher = test_broker.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @test_broker.subscriber(*args, **kwargs)
        async def m(msg) -> None:
            await publisher.publish("response")

        async with self.patch_broker(test_broker) as br:
            await br.start()
            await br.publish("hello", queue)
            publisher.mock.assert_called_with("response")

    @pytest.mark.asyncio()
    async def test_exception_raises(self, queue: str) -> None:
        test_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @test_broker.subscriber(*args, **kwargs)
        async def m(msg):  # pragma: no cover
            raise ValueError

        async with self.patch_broker(test_broker) as br:
            await br.start()

            with pytest.raises(ValueError):  # noqa: PT011
                await br.publish("hello", queue)

    @pytest.mark.asyncio()
    async def test_parser_exception_raises(self, queue: str) -> None:
        test_broker = self.get_broker()

        def parser(msg):
            raise ValueError

        args, kwargs = self.get_subscriber_params(queue, parser=parser)

        @test_broker.subscriber(*args, **kwargs)
        async def m(msg):  # pragma: no cover
            pass

        async with self.patch_broker(test_broker) as br:
            await br.start()

            with pytest.raises(ValueError):  # noqa: PT011
                await br.publish("hello", queue)

    @pytest.mark.asyncio()
    async def test_broker_gets_patched_attrs_within_cm(self, fake_producer_cls) -> None:
        test_broker = self.get_broker()
        await test_broker.start()

        old_producer = test_broker._producer

        async with self.patch_broker(test_broker) as br:
            assert isinstance(br.start, Mock)
            assert isinstance(br._connect, Mock)
            assert isinstance(br.stop, Mock)
            assert isinstance(br._producer, fake_producer_cls)

        assert not isinstance(br.start, Mock)
        assert not isinstance(br._connect, Mock)
        assert not isinstance(br.stop, Mock)
        assert br._connection is not None
        assert br._producer == old_producer

    @pytest.mark.asyncio()
    async def test_broker_with_real_doesnt_get_patched(self) -> None:
        test_broker = self.get_broker()
        await test_broker.start()

        async with self.patch_broker(test_broker, with_real=True) as br:
            assert not isinstance(br.start, Mock)
            assert not isinstance(br._connect, Mock)
            assert not isinstance(br.stop, Mock)
            assert br._connection is not None
            assert br._producer is not None

    @pytest.mark.asyncio()
    async def test_broker_with_real_patches_publishers_and_subscribers(
        self,
        queue: str,
    ) -> None:
        test_broker = self.get_broker()

        publisher = test_broker.publisher(f"{queue}1")

        args, kwargs = self.get_subscriber_params(queue)

        @test_broker.subscriber(*args, **kwargs)
        async def m(msg) -> None:
            await publisher.publish(f"response: {msg}")

        async with self.patch_broker(test_broker, with_real=True) as br:
            await br.publish("hello", queue)
            await m.wait_call(self.timeout)
            m.mock.assert_called_once_with("hello")

            with anyio.fail_after(self.timeout):
                while not publisher.mock.called:  # noqa: ASYNC110
                    await asyncio.sleep(0.1)

                publisher.mock.assert_called_once_with("response: hello")

    @pytest.mark.connected()
    @pytest.mark.asyncio()
    async def test_broker_with_real_stops_fake_subscribers(self, queue: str) -> None:
        test_broker = self.get_broker()

        publisher = test_broker.publisher(queue)  # noqa: F841

        test_client = self.patch_broker(test_broker, with_real=True)
        async with test_client:
            (fake,) = test_client._fake_subscribers
            fake.stop = spy_decorator(fake.stop)

        # A fake left running stays in its consumer group and blocks later members
        fake.stop.mock.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_publisher_response_with_model(self, queue: str) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2578."""
        from pydantic import BaseModel

        class ModelA(BaseModel):
            param1: int

        class ModelB(BaseModel):
            param2: int

        test_broker = self.get_broker(apply_types=True)

        publisher = test_broker.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)

        @test_broker.subscriber(*args, **kwargs)
        @publisher
        async def m(msg: ModelA) -> ModelB:
            return ModelB(param2=msg.param1)

        async with self.patch_broker(test_broker) as br:
            # test publish with response
            await br.publish(ModelA(param1=1), queue)

            # test request
            data = await br.request(ModelA(param1=1), queue)
            assert json.loads(data.body) == {"param2": 1}, data.body

    async def test_publisher_assert_called_once_with(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        publisher2 = broker.publisher(queue + "2")

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle() -> None:
            await publisher2.publish(
                BodyModel(name="John", age=19),
                headers={"key": "value"},
                correlation_id="cid",
            )

        args2, kwargs2 = self.get_subscriber_params(queue + "2")

        @broker.subscriber(*args2, **kwargs2)
        async def handle2(body: BodyModel) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await broker.publish("", queue)

            # The publisher answers with what its subscriber received
            await publisher2.assert_called_once_with(
                {"name": "John", "age": 19},
                headers={"key": "value"},
                correlation_id="cid",
            )
            await publisher2.assert_called_once_with(BodyModel(name="John", age=19))

            with pytest.raises(AssertionError, match=r"(?s)body:.*headers:"):
                await publisher2.assert_called_once_with(
                    {"city": "Moscow"},
                    headers={"key": "other"},
                )

        # The publisher of a real subscriber leaves the test broker with it
        with pytest.raises(SetupError, match="is not under a test broker"):
            publisher2.mock.assert_not_called()

    async def test_mock_is_only_available_under_test_broker(self, queue: str) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue + "2")

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle() -> None: ...

        with pytest.raises(SetupError, match="`handle` is not under a test broker"):
            handle.mock.assert_not_called()

        with pytest.raises(SetupError, match="is not under a test broker"):
            publisher.mock.assert_not_called()

        with pytest.raises(SetupError, match="`handle` is not under a test broker"):
            await handle.assert_called_once_with()

        with pytest.raises(SetupError, match="`handle` is not under a test broker"):
            await handle.assert_called_with()

        with pytest.raises(SetupError, match="`handle` is not under a test broker"):
            await handle.assert_any_call()

        async with self.patch_broker(broker):
            handle.mock.assert_not_called()
            publisher.mock.assert_not_called()

        # Leaving the test broker takes the mock away again
        with pytest.raises(SetupError, match="`handle` is not under a test broker"):
            handle.mock.assert_not_called()

    async def test_subscriber_assertion_checks_body_fields_and_context(
        self, queue: str
    ) -> None:
        broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(body: BodyModel) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await broker.publish(
                BodyModel(name="John", age=19),
                queue,
                headers={"key": "value"},
                correlation_id="cid",
            )

            # Headers match as a subset: the framework adds its own beside `key`
            await handle.assert_called_once_with(
                {"name": "John", "age": 19},
                headers={"key": "value"},
                correlation_id="cid",
                context={"broker": broker, "message.correlation_id": "cid"},
            )
            await handle.assert_called_once_with(BodyModel(name="John", age=19))
            await handle.assert_called_once_with(IsPartialDict(name="John"))

            # Every mismatch is reported at once, not just the first one
            with pytest.raises(AssertionError, match=r"(?s)body:.*headers:"):
                await handle.assert_called_once_with(
                    {"city": "Moscow"},
                    headers={"key": "other"},
                )

    async def test_subscriber_assert_called_with_reads_the_last_call(
        self, queue: str
    ) -> None:
        broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(body: BodyModel) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await broker.publish(
                BodyModel(name="John", age=19), queue, headers={"n": "1"}
            )
            await broker.publish(
                BodyModel(name="Jane", age=20), queue, headers={"n": "2"}
            )

            await handle.assert_called_with(
                {"name": "Jane", "age": 20}, headers={"n": "2"}
            )

            with pytest.raises(AssertionError, match=r"(?s)body:.*headers:"):
                await handle.assert_called_with(
                    {"name": "John", "age": 19},
                    headers={"n": "1"},
                )

            # The count is still the mock's business
            with pytest.raises(AssertionError, match="Called 2 times"):
                await handle.assert_called_once_with({"name": "Jane", "age": 20})

    async def test_subscriber_assert_any_call_searches_every_call(
        self, queue: str
    ) -> None:
        broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(body: BodyModel) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await broker.publish(
                BodyModel(name="John", age=19), queue, headers={"n": "1"}
            )
            await broker.publish(
                BodyModel(name="Jane", age=20), queue, headers={"n": "2"}
            )

            await handle.assert_any_call({"name": "John", "age": 19}, headers={"n": "1"})
            await handle.assert_any_call({"name": "Jane", "age": 20}, headers={"n": "2"})

            # Every recorded call is listed with its own mismatches
            with pytest.raises(
                AssertionError,
                match=r"(?s)call 1:.*body:.*headers:.*call 2:.*body:.*headers:",
            ):
                await handle.assert_any_call({"city": "Moscow"}, headers={"n": "3"})

    async def test_assertions_fail_on_an_endpoint_nobody_called(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle() -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()

            for assertion in (
                handle.assert_called_once_with,
                handle.assert_called_with,
                handle.assert_any_call,
            ):
                with pytest.raises(AssertionError, match="`handle` was not called"):
                    await assertion("hello")

    async def test_publisher_assertions_share_the_recorded_calls(
        self, queue: str
    ) -> None:
        broker = self.get_broker(apply_types=True)

        publisher = broker.publisher(queue + "2")

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle() -> None:
            await publisher.publish(BodyModel(name="John", age=19), correlation_id="1")
            await publisher.publish(BodyModel(name="Jane", age=20), correlation_id="2")

        async with self.patch_broker(broker) as br:
            await br.start()
            await broker.publish("", queue)

            await publisher.assert_called_with(
                {"name": "Jane", "age": 20}, correlation_id="2"
            )
            await publisher.assert_any_call(
                {"name": "John", "age": 19}, correlation_id="1"
            )

            with pytest.raises(AssertionError, match="Called 2 times"):
                await publisher.assert_called_once_with({"name": "Jane", "age": 20})


class _BatchCodec(DefaultCodec):
    async def encode_batch(
        self,
        msgs: "Sequence[Any]",
        serializer: Any = None,
    ) -> list[tuple[bytes, str | None]]:
        return [await self.encode(m, serializer) for m in msgs]

    async def decode_batch(self, msg: Any) -> list[Any]:
        return list(msg.body)


class _TrackingCodec(DefaultCodec):
    def __init__(self) -> None:
        self.encoded: list[Any] = []

    async def encode(
        self,
        msg: Any,
        serializer: Any = None,
    ) -> tuple[bytes, str | None]:
        self.encoded.append(msg)
        return await super().encode(msg, serializer)


# NOTE: kafka/confluent only - other brokers have no tombstone concept.
@pytest.mark.asyncio()
class KafkaTombstoneTestclientTestcase(BaseTestcaseConfig):
    response_cls: type[Any]

    @staticmethod
    def get_message_value(raw_message: Any) -> bytes | None:
        return raw_message.value

    async def test_tombstone_body_reads_as_empty_bytes(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        bodies: asyncio.Queue[bytes] = asyncio.Queue()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: bytes) -> None:
            await bodies.put(msg)

        async with self.patch_broker(broker) as br:
            await br.publish(TOMBSTONE, queue, key=b"tombstone-key")
            body = await asyncio.wait_for(bodies.get(), timeout=self.timeout)

        assert body == b""
        assert isinstance(body, Tombstone)

    async def test_publish_tombstone_sends_a_real_tombstone(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any = Context("message")) -> None:
            await values.put(self.get_message_value(msg.raw_message))

        async with self.patch_broker(broker) as br:
            await br.publish(TOMBSTONE, queue, key=b"tombstone-key")
            value = await asyncio.wait_for(values.get(), timeout=self.timeout)

        assert value is None

    async def test_request_with_tombstone(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any = Context("message")) -> str:
            await values.put(self.get_message_value(msg.raw_message))
            return "pong"

        async with self.patch_broker(broker) as br:
            response = await br.request(TOMBSTONE, queue, key=b"tombstone-key")
            value = await asyncio.wait_for(values.get(), timeout=self.timeout)

        assert value is None
        assert await response.decode() == "pong"

    async def test_publish_tombstone_without_key_raises(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            with pytest.raises(ValueError, match="requires a key"):
                await br.publish(TOMBSTONE, queue)

    async def test_tombstone_never_reaches_a_custom_codec(self, queue: str) -> None:
        codec = _TrackingCodec()
        broker = self.get_broker(apply_types=True, codec=codec)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any = Context("message")) -> None:
            await values.put(self.get_message_value(msg.raw_message))

        async with self.patch_broker(broker) as br:
            await br.publish(TOMBSTONE, queue, key=b"tombstone-key")
            value = await asyncio.wait_for(values.get(), timeout=self.timeout)

        assert value is None
        # the handler's own `None` return still goes through the codec; the
        # tombstone never does
        assert not any(isinstance(msg, Tombstone) for msg in codec.encoded)

    async def test_publish_batch_with_tombstone(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        values: list[bytes] = []

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: list[bytes]) -> None:
            values.extend(msg)

        async with self.patch_broker(broker) as br:
            await br.publish_batch(
                b"hi",
                self.response_cls(TOMBSTONE, key=b"batch-tombstone-key"),
                topic=queue,
            )

        assert values == [b"hi", b""]
        assert [isinstance(value, Tombstone) for value in values] == [False, True]

    async def test_plain_none_in_a_batch_is_not_a_tombstone(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        values: list[bytes] = []
        raw_values: list[bytes | None] = []

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @broker.subscriber(*args, **kwargs)
        async def handler(
            msg: list[bytes],
            raw: Any = Context("message"),
        ) -> None:
            values.extend(msg)
            raw_values.extend(self.get_message_value(m) for m in raw.raw_message)

        async with self.patch_broker(broker) as br:
            await br.publish_batch(b"hi", None, topic=queue)

        # a None body encodes like any other value, so nothing on the wire is
        # null and nothing arrives as a tombstone
        assert None not in raw_values
        assert not any(isinstance(value, Tombstone) for value in values)

    async def test_batch_tombstone_with_custom_batch_codec_raises(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(codec=_BatchCodec())

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: list[bytes]) -> None: ...

        async with self.patch_broker(broker) as br:
            with pytest.raises(ValueError, match="BatchCodecProto"):
                await br.publish_batch(
                    b"hi",
                    self.response_cls(TOMBSTONE, key=b"batch-tombstone-key"),
                    topic=queue,
                )
