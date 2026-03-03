from dataclasses import dataclass
from typing import Any

import pytest

from faststream.message.utils import decode_message


@dataclass
class _MessageStub:
    body: bytes
    content_type: str | None = None


def test_decode_text_ok() -> None:
    msg: Any = _MessageStub(b"faststream", "text/plain")

    assert decode_message(msg) == "faststream"


@pytest.mark.parametrize(
    "content_type",
    (
        "application/json",
        "application/json; charset=utf-8",
    ),
)
def test_decode_json_ok(content_type: str) -> None:
    msg: Any = _MessageStub(b'{"key": "value"}', content_type)

    assert decode_message(msg) == {"key": "value"}


@pytest.mark.parametrize(
    "content_type",
    (
        "application/octet-stream",
        "unknown",
        "audio/ogg",
        "video/mp4",
        "image/jpeg",
    ),
)
def test_unknown_content_type_ok(content_type: str) -> None:
    msg: Any = _MessageStub(b"faststream", content_type)

    assert decode_message(msg) == b"faststream"


@pytest.mark.parametrize(
    ("body", "expected"),
    (
        pytest.param(b"faststream", b"faststream", id="raw_invalid_json"),
        pytest.param(b'{"key": "value"}', {"key": "value"}, id="raw_valid_json"),
        pytest.param(_MessageStub(b"faststream"), b"faststream", id="invalid_json"),
        pytest.param(
            _MessageStub(b'{"key": "value"}'), {"key": "value"}, id="valid_json"
        ),
    ),
)
def test_no_content_type_ok(body: Any, expected: bytes | dict[str, str]) -> None:
    assert decode_message(body) == expected
