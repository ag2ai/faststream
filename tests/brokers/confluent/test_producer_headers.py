from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from faststream.confluent.helpers.client import AsyncConfluentProducer
from faststream.confluent.publisher.producer import AsyncConfluentFastProducerImpl
from faststream.confluent.response import KafkaPublishCommand
from faststream.response.publish_type import PublishType


@pytest.mark.confluent()
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
    driver.send = AsyncMock()
    driver.send_batch = AsyncMock()

    producer = AsyncConfluentFastProducerImpl(parser=None, decoder=None)
    producer.connect(driver, serializer=None)
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
            no_confirm=False,
        )


@pytest.mark.confluent()
@pytest.mark.asyncio()
@pytest.mark.parametrize("batch", (False, True))
async def test_native_producer_receives_header_sequences_as_lists(*, batch: bool) -> None:
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
            if batch:
                builder = producer.create_batch()
                builder.append(value=b"payload", headers=headers)
                await producer.send_batch(builder, "headers", partition=None)
            else:
                await producer.send("headers", value=b"payload", headers=headers)
        finally:
            await producer.stop()

    assert [call.kwargs["headers"] for call in native.produce.call_args_list] == [
        [("binary", b"\xff"), ("empty", b""), ("null", None)],
    ]
