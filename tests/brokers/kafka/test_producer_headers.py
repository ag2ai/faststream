import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from faststream.kafka.publisher.producer import AioKafkaFastProducerImpl
from faststream.kafka.response import KafkaPublishCommand
from faststream.response.publish_type import PublishType


@pytest.mark.kafka()
@pytest.mark.asyncio()
@pytest.mark.parametrize("batch", (False, True))
async def test_producer_preserves_binary_and_null_headers(*, batch: bool) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2214."""
    headers: dict[str, str | bytes | None] = {
        "text": "привет",
        "utf8": "привет".encode(),
        "binary": b"\xff\x00\x80",
        "empty-text": "",
        "empty-bytes": b"",
        "null": None,
    }
    encoded_headers = [
        ("text", "привет".encode()),
        ("utf8", "привет".encode()),
        ("binary", b"\xff\x00\x80"),
        ("empty-text", b""),
        ("empty-bytes", b""),
        ("null", None),
    ]
    driver: Any = MagicMock()
    driver.start = AsyncMock()
    result: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    result.set_result(None)
    driver.send = AsyncMock(return_value=result)
    driver.send_batch = AsyncMock(return_value=result)

    producer = AioKafkaFastProducerImpl(parser=None, decoder=None)
    await producer.connect(driver, serializer=None)
    cmd = KafkaPublishCommand(
        b"payload",
        *(({"value": 1},) if batch else ()),
        topic="headers",
        headers=headers,
        _publish_type=PublishType.PUBLISH,
    )

    if batch:
        await producer.publish_batch(cmd)
        assert [
            call.kwargs["headers"]
            for call in driver.create_batch.return_value.append.call_args_list
        ] == [
            encoded_headers,
            [("content-type", b"application/json"), *encoded_headers],
        ]
    else:
        await producer.publish(cmd)
        driver.send.assert_awaited_once_with(
            topic="headers",
            value=b"payload",
            key=None,
            partition=None,
            timestamp_ms=None,
            headers=[("content-type", b""), *encoded_headers],
        )
