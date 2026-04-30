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
