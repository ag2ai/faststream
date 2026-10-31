from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from faststream.confluent.helpers.client import AsyncConfluentProducer
from faststream.confluent.publisher.producer import AsyncConfluentFastProducerImpl
from faststream.confluent.response import KafkaPublishCommand
from faststream.response.publish_type import PublishType

HEADERS: dict[str, str | bytes | None] = {
    "text": "привет",
    "utf8": "привет".encode(),
    "binary": b"\xff\x00\x80",
    "empty-text": "",
    "empty-bytes": b"",
    "null": None,
}
ENCODED_HEADERS = [
    ("text", "привет".encode()),
    ("utf8", "привет".encode()),
    ("binary", b"\xff\x00\x80"),
    ("empty-text", b""),
    ("empty-bytes", b""),
    ("null", None),
]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_producer_publish_preserves_binary_and_null_headers() -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2214."""
    driver: Any = MagicMock()
    driver.send = AsyncMock()

    producer = AsyncConfluentFastProducerImpl(parser=None, decoder=None)
    producer.connect(driver, serializer=None)
    cmd = KafkaPublishCommand(
        b"payload",
        topic="headers",
        headers=HEADERS,
        _publish_type=PublishType.PUBLISH,
    )

    await producer.publish(cmd)
    driver.send.assert_awaited_once_with(
        topic="headers",
        value=b"payload",
        key=None,
        partition=None,
        timestamp_ms=None,
        headers=[("content-type", b""), *ENCODED_HEADERS],
        no_confirm=False,
    )


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_producer_publish_batch_preserves_binary_and_null_headers() -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2214."""
    driver: Any = MagicMock()
    driver.send_batch = AsyncMock()

    producer = AsyncConfluentFastProducerImpl(parser=None, decoder=None)
    producer.connect(driver, serializer=None)
    cmd = KafkaPublishCommand(
        b"payload",
        {"value": 1},
        topic="headers",
        headers=HEADERS,
        _publish_type=PublishType.PUBLISH,
    )

    await producer.publish_batch(cmd)
    assert [
        call.kwargs["headers"]
        for call in driver.create_batch.return_value.append.call_args_list
    ] == [
        ENCODED_HEADERS,
        [("content-type", b"application/json"), *ENCODED_HEADERS],
    ]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_native_producer_send_receives_header_sequences_as_lists() -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2214."""
    headers = (("binary", b"\xff"), ("empty", b""), ("null", None))
    native = MagicMock()

    def acknowledge(_topic: str, **kwargs: Any) -> None:
        kwargs["on_delivery"](None, None)

    native.produce.side_effect = acknowledge
    config: Any = MagicMock()
    logger: Any = MagicMock()

    with patch("faststream.confluent.helpers.client.Producer", return_value=native):
        producer = AsyncConfluentProducer(logger=logger, config=config)
        try:
            await producer.send("headers", value=b"payload", headers=headers)
        finally:
            await producer.stop()

    assert [call.kwargs["headers"] for call in native.produce.call_args_list] == [
        [("binary", b"\xff"), ("empty", b""), ("null", None)],
    ]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_native_producer_send_batch_receives_header_sequences_as_lists() -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2214."""
    headers = (("binary", b"\xff"), ("empty", b""), ("null", None))
    native = MagicMock()

    def acknowledge(_topic: str, **kwargs: Any) -> None:
        kwargs["on_delivery"](None, None)

    native.produce.side_effect = acknowledge
    config: Any = MagicMock()
    logger: Any = MagicMock()

    with patch("faststream.confluent.helpers.client.Producer", return_value=native):
        producer = AsyncConfluentProducer(logger=logger, config=config)
        try:
            builder = producer.create_batch()
            builder.append(value=b"payload", headers=headers)
            await producer.send_batch(builder, "headers", partition=None)
        finally:
            await producer.stop()

    assert [call.kwargs["headers"] for call in native.produce.call_args_list] == [
        [("binary", b"\xff"), ("empty", b""), ("null", None)],
    ]
