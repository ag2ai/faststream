from collections.abc import Sequence
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream._internal.parser import DefaultCodec
from tests.brokers.base.codec import BatchCodecTestcase, CodecTestcase

from .basic import SQSMemoryTestcaseConfig


@pytest.mark.sqs()
@pytest.mark.asyncio()
class TestSQSCodec(SQSMemoryTestcaseConfig, CodecTestcase):
    pass


@pytest.mark.sqs()
@pytest.mark.asyncio()
class TestSQSBatchCodec(SQSMemoryTestcaseConfig, BatchCodecTestcase):
    # The base testcase publishes with the kafka-style `topic=` kwarg;
    # SQS's `publish_batch` names its destination `queue=`.
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
            await br.publish_batch("a", "b", "c", queue=queue)

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
            await br.publish_batch("a", "b", "c", queue=queue)

        assert encode_batch_mock.called, "encode_batch was not called on publish"
