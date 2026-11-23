from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import MessageSizeTooLargeError
from aiokafka.record.default_records import DefaultRecordBatchBuilder
from aiokafka.record.legacy_records import LegacyRecordBatchBuilder

from faststream._internal.kafka.tombstone import Tombstone


class AioKafkaDriver(AIOKafkaProducer):  # type: ignore[misc]
    """Prepare explicit tombstone batches before they can enqueue any records."""

    def __init__(
        self,
        *,
        key_serializer: Callable[[Any], bytes] | None = None,
        value_serializer: Callable[[Any], bytes] | None = None,
        max_request_size: int = 1024 * 1024,
        **kwargs: Any,
    ) -> None:
        self._batch_key_serializer = key_serializer
        self._batch_value_serializer = value_serializer
        self._batch_max_request_size = max_request_size
        super().__init__(
            key_serializer=partial(_serialize, serializer=key_serializer)
            if key_serializer is not None
            else None,
            value_serializer=partial(_serialize_value, serializer=value_serializer),
            max_request_size=max_request_size,
            **kwargs,
        )

    def prepare_record(
        self,
        *,
        key: Any,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> tuple[Any, Any]:
        key_serializer = self._batch_key_serializer
        value_serializer = self._batch_value_serializer
        wire_key = _serialize(key, serializer=key_serializer)
        wire_value = _serialize_value(value, serializer=value_serializer)
        # Validate buffer types before enqueueing any record.
        for payload in (wire_key, wire_value):
            if payload is not None:
                memoryview(payload)

        if wire_key is None and wire_value is None:
            msg = "a null Kafka value requires a key that serializes to bytes"
            raise ValueError(msg)

        # aiokafka's legacy client uses a legacy size limit, excluding headers.
        if (api_version := getattr(self.client, "api_version", None)) is not None:
            overhead = LegacyRecordBatchBuilder.record_overhead(
                int(api_version >= (0, 10))
            )
            size = overhead + len(wire_key or b"") + len(wire_value or b"")
        else:
            size = DefaultRecordBatchBuilder.estimate_size_in_bytes(
                wire_key, wire_value, headers
            )
        if size > self._batch_max_request_size:
            msg = (
                f"The serialized Kafka record is {size} bytes, exceeding max_request_size"
            )
            raise MessageSizeTooLargeError(msg)
        return (
            key if key_serializer is None else _Serialized(wire_key),
            value
            if value_serializer is None or isinstance(value, Tombstone)
            else _Serialized(wire_value),
        )


@dataclass(frozen=True)
class _Serialized:
    value: bytes | None


def _serialize(value: Any, *, serializer: Callable[[Any], bytes] | None) -> bytes | None:
    if isinstance(value, _Serialized):
        return value.value
    return value if serializer is None else serializer(value)


def _serialize_value(
    value: Any, *, serializer: Callable[[Any], bytes] | None
) -> bytes | None:
    if isinstance(value, Tombstone):
        return None
    return _serialize(value, serializer=serializer)
