from __future__ import annotations

from secrets import token_hex
from typing import Any

MQ_ID_BYTES_LENGTH = 24
MQ_ID_HEX_LENGTH = MQ_ID_BYTES_LENGTH * 2


def generate_mq_id() -> str:
    return token_hex(MQ_ID_BYTES_LENGTH)


def normalize_mq_id(value: str | None, *, field_name: str) -> str | None:
    if value in (None, ""):
        return None

    try:
        raw = bytes.fromhex(value)
    except ValueError as e:
        raise ValueError(_mq_id_error(field_name)) from e

    if len(raw) != MQ_ID_BYTES_LENGTH:
        raise ValueError(_mq_id_error(field_name))

    return raw.hex()


def mq_id_to_bytes(value: str | None, *, field_name: str) -> bytes | None:
    normalized = normalize_mq_id(value, field_name=field_name)
    if normalized is None:
        return None
    return bytes.fromhex(normalized)


def try_parse_mq_id(value: Any) -> str | None:
    raw: bytes | None = None

    if isinstance(value, str):
        try:
            raw = bytes.fromhex(value)
        except ValueError:
            return None

    elif isinstance(value, bytes):
        raw = value

    elif isinstance(value, bytearray):
        raw = bytes(value)

    if not raw or len(raw) != MQ_ID_BYTES_LENGTH or not any(raw):
        return None

    return raw.hex()


def _mq_id_error(field_name: str) -> str:
    return f"`{field_name}` must be a {MQ_ID_HEX_LENGTH}-character hex string."
