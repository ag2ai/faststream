from collections.abc import Sequence
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream._internal.parser import DefaultCodec
from faststream.message.utils import encode_message
from tests.brokers.base.basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class CodecTestcase(BaseTestcaseConfig):
    async def test_codec_decode_called(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        class TrackingCodec(DefaultCodec):
            async def decode(self, msg):
                mock()
                return await super().decode(msg)

        codec = TrackingCodec()
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue, codec=codec)

        @broker.subscriber(*args, **kwargs)
        async def handle(m) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish(b"hello", queue)

        mock.assert_called_once()

    async def test_codec_not_set_uses_default(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(m) -> None:
            mock(m)

        async with self.patch_broker(broker) as br:
            await br.publish({"key": "value"}, queue)

        mock.assert_called_once_with({"key": "value"})

    async def test_codec_and_decoder_conflict_raises(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()
        codec = DefaultCodec()

        async def my_decoder(msg, original):
            return await original(msg)

        args, kwargs = self.get_subscriber_params(queue, codec=codec, decoder=my_decoder)

        @broker.subscriber(*args, **kwargs)
        async def handle(m) -> None:
            pass  # pragma: no cover

        # ValueError raised inside _get_parser_and_decoder() during start(),
        # which TestBroker.__aenter__ calls before yielding — hence it propagates
        # from the "async with" expression rather than from the body.
        with pytest.raises(ValueError, match="codec"):
            async with self.patch_broker(broker):
                pass  # pragma: no cover

    async def test_broker_level_codec(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        class TrackingCodec(DefaultCodec):
            async def decode(self, msg):
                mock()
                return await super().decode(msg)

        broker = self.get_broker(codec=TrackingCodec())

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(m) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish(b"hello", queue)

        mock.assert_called_once()

    async def test_codec_encode_called(self, queue: str) -> None:
        mock = MagicMock()

        class TrackingCodec(DefaultCodec):
            async def encode(self, msg, serializer=None):
                mock()
                return await super().encode(msg, serializer)

        broker = self.get_broker(codec=TrackingCodec())

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handle(m) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish({"key": "value"}, queue)

        assert mock.called, "codec.encode was not called on publish"

    async def test_default_codec_encode_matches_encode_message(self, queue: str) -> None:
        codec = DefaultCodec()

        test_cases = [
            None,
            b"raw bytes",
            "hello string",
            {"json": True, "value": 42},
        ]

        for msg in test_cases:
            codec_result = await codec.encode(msg, None)
            direct_result = encode_message(msg, None)
            assert codec_result == direct_result, (
                f"DefaultCodec.encode({msg!r}) = {codec_result!r} "
                f"but encode_message({msg!r}) = {direct_result!r}"
            )


@pytest.mark.asyncio()
class BatchCodecTestcase(BaseTestcaseConfig):
    async def test_batch_codec_decode_batch_called(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        decode_batch_mock = MagicMock()

        class TrackingBatchCodec(DefaultCodec):
            async def encode_batch(
                self,
                msgs: Sequence[Any],
                serializer: Any = None,
            ) -> list[tuple[bytes, str | None]]:
                return [await DefaultCodec.encode(self, m, serializer) for m in msgs]

            async def decode_batch(self, msg: Any) -> list[Any]:
                decode_batch_mock()
                return [b.decode() if isinstance(b, bytes) else b for b in msg.body]

        codec = TrackingBatchCodec()
        broker = self.get_broker(codec=codec)

        @broker.subscriber(queue, batch=True)
        async def handle(m: list[str]) -> None:
            mock(m)

        async with self.patch_broker(broker) as br:
            await br.publish_batch("a", "b", "c", topic=queue)

        assert decode_batch_mock.called, "decode_batch was not called"
        mock.assert_called_once_with(["a", "b", "c"])

    async def test_batch_codec_encode_batch_called(
        self,
        queue: str,
    ) -> None:
        encode_batch_mock = MagicMock()

        class TrackingBatchCodec(DefaultCodec):
            async def encode_batch(
                self,
                msgs: Sequence[Any],
                serializer: Any = None,
            ) -> list[tuple[bytes, str | None]]:
                encode_batch_mock()
                return [await DefaultCodec.encode(self, m, serializer) for m in msgs]

            async def decode_batch(self, msg: Any) -> list[Any]:
                return [b.decode() if isinstance(b, bytes) else b for b in msg.body]

        broker = self.get_broker(codec=TrackingBatchCodec())

        @broker.subscriber(queue, batch=True)
        async def handle(m: list[str]) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish_batch("a", "b", "c", topic=queue)

        assert encode_batch_mock.called, "encode_batch was not called on publish"

    async def test_batch_codec_without_batch_proto_raises(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(codec=DefaultCodec())

        @broker.subscriber(queue, batch=True)
        async def handle(m: list[str]) -> None:
            pass

        with pytest.raises(ValueError, match="BatchCodecProto"):
            async with self.patch_broker(broker):
                pass

    async def test_batch_codec_isinstance_dispatch(self) -> None:
        from faststream._internal.parser import BatchCodecProto

        class WithBatch(DefaultCodec):
            async def encode_batch(
                self,
                msgs: Sequence[Any],
                serializer: Any = None,
            ) -> list[tuple[bytes, str | None]]:
                return [await super().encode(m, serializer) for m in msgs]

            async def decode_batch(self, msg: Any) -> list[Any]:
                return []

        class WithoutBatch(DefaultCodec):
            pass

        assert isinstance(WithBatch(), BatchCodecProto)
        assert not isinstance(WithoutBatch(), BatchCodecProto)
