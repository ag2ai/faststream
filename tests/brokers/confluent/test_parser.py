from typing import TYPE_CHECKING, cast

import pytest
from dirty_equals import IsUUID

if TYPE_CHECKING:
    from confluent_kafka import Message

    from faststream.message import StreamMessage

from faststream.confluent.parser import AsyncConfluentParser
from faststream.confluent.testing import MockConfluentMessage
from tests.brokers.base.parser import CustomParserTestcase

from .basic import ConfluentTestcaseConfig


@pytest.mark.connected()
@pytest.mark.confluent()
class TestCustomParser(ConfluentTestcaseConfig, CustomParserTestcase):
    pass


def make_message(headers: list[tuple[str, str | bytes | None]]) -> MockConfluentMessage:
    return MockConfluentMessage(
        raw_msg=b'{"value": 1}',
        topic="test-topic",
        key=b"",
        headers=headers,
        offset=0,
        partition=0,
        timestamp_type=0,
        timestamp_ms=0,
    )


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_parse_non_utf8_headers() -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2458."""
    parser = AsyncConfluentParser()
    result = await parser.parse_message(
        make_message([
            ("text", "привет"),
            ("binary", b"\xc3\x28"),
            ("empty", b""),
            ("nullable", None),
            ("reply_to", b"reply-\xff"),
            ("content-type", b"type-\xff"),
            ("correlation_id", b"id-\xff"),
        ])
    )

    assert (
        result.headers,
        result.reply_to,
        result.content_type,
        result.correlation_id,
    ) == (
        {
            "text": "привет",
            "binary": "\ufffd(",
            "empty": "",
            "reply_to": "reply-\ufffd",
            "content-type": "type-\ufffd",
            "correlation_id": "id-\ufffd",
        },
        "reply-\ufffd",
        "type-\ufffd",
        "id-\ufffd",
    )


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_parse_batch_non_utf8_headers() -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2458."""
    parser = AsyncConfluentParser()
    result = await parser.parse_batch((
        make_message([("content-type", b"application/json"), ("text", b"first")]),
        make_message([
            ("content-type", b"application/json"),
            ("binary", b"\xc3\x28"),
            ("nullable", None),
        ]),
    ))

    assert (
        result.headers,
        result.batch_headers,
        await parser.decode_batch(cast("StreamMessage[tuple[Message, ...]]", result)),
    ) == (
        {"content-type": "application/json", "text": "first"},
        [
            {"content-type": "application/json", "text": "first"},
            {"content-type": "application/json", "binary": "\ufffd("},
        ],
        [{"value": 1}, {"value": 1}],
    )


@pytest.mark.confluent()
@pytest.mark.asyncio()
@pytest.mark.parametrize("batch", (False, True), ids=("single", "batch"))
@pytest.mark.parametrize(
    ("header", "value", "decoded"),
    (
        pytest.param(None, None, None, id="absent"),
        pytest.param("reply_to", None, None, id="reply-null"),
        pytest.param("reply_to", b"", "", id="reply-empty"),
        pytest.param("reply_to", b"reply-\xff", "reply-\ufffd", id="reply-binary"),
        pytest.param("content-type", None, None, id="content-null"),
        pytest.param("content-type", b"", "", id="content-empty"),
        pytest.param("content-type", b"type-\xff", "type-\ufffd", id="content-binary"),
        pytest.param("correlation_id", None, None, id="correlation-null"),
        pytest.param("correlation_id", b"", "", id="correlation-empty"),
        pytest.param("correlation_id", b"id-\xff", "id-\ufffd", id="correlation-binary"),
    ),
)
async def test_parse_reserved_headers(
    header: str | None,
    value: bytes | None,
    decoded: str | None,
    batch: bool,
) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/2458."""
    parser = AsyncConfluentParser()
    raw_headers: list[tuple[str, str | bytes | None]] = (
        [(header, value)] if header is not None else []
    )
    message = make_message(raw_headers)
    other_headers = {
        "reply_to": "other-reply",
        "content-type": "other-type",
        "correlation_id": "other-id",
    }
    result = (
        await parser.parse_batch((
            message,
            make_message([(key, value.encode()) for key, value in other_headers.items()]),
        ))
        if batch
        else await parser.parse_message(message)
    )
    expected_headers = (
        {header: decoded} if header is not None and decoded is not None else {}
    )

    assert (
        result.headers,
        result.batch_headers,
        result.reply_to,
        result.content_type,
        result.correlation_id,
    ) == (
        expected_headers,
        [expected_headers, other_headers] if batch else [],
        decoded if header == "reply_to" and decoded is not None else "",
        decoded if header == "content-type" else None,
        decoded if header == "correlation_id" and decoded else IsUUID(version=4),
    )
