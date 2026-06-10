import asyncio
import base64
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Optional, cast

import anyio
from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import BatchCodecProto, DefaultCodec
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException, IncorrectState
from faststream.message import gen_cor_id
from faststream.sqs.exceptions import (
    MAX_BATCH_ENTRIES,
    MAX_MESSAGE_ATTRIBUTES,
    MAX_MESSAGE_SIZE,
    BatchSendError,
    FifoQueueError,
    MessageTooLargeError,
    TooManyMessageAttributesError,
)
from faststream.sqs.parser import (
    BASE64_BODY_ATTR,
    EMPTY_BODY_ATTR,
    EMPTY_BODY_PLACEHOLDER,
    SQSParser,
)
from faststream.sqs.response import SQSPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto
    from types_aiobotocore_sqs import SQSClient

    from faststream._internal.parser import CodecProto
    from faststream._internal.types import AsyncCallable, CustomCallable

    from .config import SQSPublisherConfig  # noqa: F401


class SQSFastProducer(ProducerProto[SQSPublishCommand]):
    """Producer that sends messages to SQS queues via aiobotocore."""

    _parser: "AsyncCallable"
    _decoder: "AsyncCallable"

    def __init__(
        self,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
    ) -> None:
        self.serializer: SerializerProto | None = None
        self._client: SQSClient | None = None
        self._queue_urls: dict[str, str] = {}
        self.codec: CodecProto = DefaultCodec()

        # RPC state
        self.response_queue_url: str | None = None
        self._futures: dict[str, asyncio.Future[Any]] = {}

        default_parser = SQSParser()
        self._parser = ParserComposition(parser, default_parser.parse_message)
        self._decoder = ParserComposition(decoder, default_parser.decode_message)

    def connect(
        self,
        client: "SQSClient",
        serializer: Optional["SerializerProto"],
        codec: Optional["CodecProto"] = None,
        queue_urls: dict[str, str] | None = None,
    ) -> None:
        self._client = client
        self.serializer = serializer
        self.codec = codec or DefaultCodec()
        if queue_urls is not None:
            self._queue_urls = queue_urls

    def disconnect(self) -> None:
        self._client = None
        self.serializer = None
        self.response_queue_url = None
        for future in self._futures.values():
            if not future.done():
                future.cancel()
        self._futures.clear()

    @property
    def _connected_client(self) -> "SQSClient":
        if self._client is None:
            msg = "Producer is not connected. Call connect() first."
            raise IncorrectState(msg)
        return self._client

    async def get_queue_url(self, queue: str) -> str:
        # reply_to / declared queues already arrive as full URLs
        if queue.startswith(("http://", "https://")):
            return queue
        if queue in self._queue_urls:
            return self._queue_urls[queue]
        client = self._connected_client
        try:
            resp = await client.get_queue_url(QueueName=queue)
        except client.exceptions.QueueDoesNotExist:
            # auto-create on publish, mirroring topic auto-creation in other brokers
            attributes = {"FifoQueue": "true"} if queue.endswith(".fifo") else {}
            resp = await client.create_queue(
                QueueName=queue,
                Attributes=attributes,  # type: ignore[arg-type]
            )
        url = resp["QueueUrl"]
        self._queue_urls[queue] = url
        return url

    def _build_message_attributes(
        self,
        cmd: "SQSPublishCommand",
        content_type: str | None,
    ) -> dict[str, Any]:
        attrs: dict[str, Any] = {}
        for key, value in (cmd.headers or {}).items():
            attrs[key] = {"DataType": "String", "StringValue": str(value)}
        if content_type:
            attrs["content-type"] = {"DataType": "String", "StringValue": content_type}
        if cmd.reply_to:
            attrs["reply_to"] = {"DataType": "String", "StringValue": cmd.reply_to}
        if cmd.correlation_id:
            attrs["correlation_id"] = {
                "DataType": "String",
                "StringValue": cmd.correlation_id,
            }
        return attrs

    @staticmethod
    def _attributes_size(attrs: dict[str, Any]) -> int:
        """Approximate the bytes SQS counts for ``MessageAttributes``.

        SQS counts attribute names, data types and values against the 256 KiB
        message-size limit. This is a close upper bound, not the exact wire size.
        """
        total = 0
        for name, attr in attrs.items():
            total += len(name.encode())
            total += len(str(attr.get("DataType", "")).encode())
            value = attr.get("StringValue", attr.get("BinaryValue", ""))
            if isinstance(value, (bytes, bytearray)):
                total += len(value)
            else:
                total += len(str(value).encode())
        return total

    def _validate_message(self, body: str, attrs: dict[str, Any]) -> None:
        if len(attrs) > MAX_MESSAGE_ATTRIBUTES:
            raise TooManyMessageAttributesError(len(attrs))

        size = len(body.encode()) + self._attributes_size(attrs)
        if size > MAX_MESSAGE_SIZE:
            raise MessageTooLargeError(size)

    @staticmethod
    def _validate_fifo(cmd: "SQSPublishCommand") -> None:
        """FIFO queues require a ``MessageGroupId`` on every message."""
        if cmd.queue.endswith(".fifo") and not cmd.group_id:
            msg = (
                f"FIFO queue '{cmd.queue}' requires a `group_id` (MessageGroupId). "
                "Pass `group_id=...` to publish()/the publisher."
            )
            raise FifoQueueError(msg)

    def _build_entry(
        self,
        payload: Any,
        content_type: str | None,
        cmd: "SQSPublishCommand",
    ) -> tuple[str, dict[str, Any]]:
        """Turn an encoded payload into a validated (MessageBody, attributes) pair.

        SQS accepts only text bodies, so non-UTF-8 bytes are shipped base64-encoded
        and flagged with a reserved attribute (restored by ``SQSParser``); empty
        bodies are replaced with a flagged placeholder the same way.
        """
        attrs = self._build_message_attributes(cmd, content_type)

        if isinstance(payload, bytes):
            try:
                body = payload.decode()
            except UnicodeDecodeError:
                body = base64.b64encode(payload).decode()
                attrs[BASE64_BODY_ATTR] = {"DataType": "String", "StringValue": "1"}
        else:
            body = str(payload)

        if not body:
            body = EMPTY_BODY_PLACEHOLDER
            attrs[EMPTY_BODY_ATTR] = {"DataType": "String", "StringValue": "1"}

        self._validate_message(body, attrs)
        return body, attrs

    async def _build_send_kwargs(self, cmd: "SQSPublishCommand") -> dict[str, Any]:
        self._validate_fifo(cmd)
        payload, content_type = await self.codec.encode(cmd.body, self.serializer)
        body, attrs = self._build_entry(payload, content_type, cmd)

        kwargs: dict[str, Any] = {
            "QueueUrl": await self.get_queue_url(cmd.queue),
            "MessageBody": body,
            "MessageAttributes": attrs,
        }
        if cmd.delay_seconds:
            kwargs["DelaySeconds"] = cmd.delay_seconds
        if cmd.group_id:
            kwargs["MessageGroupId"] = cmd.group_id
        if cmd.deduplication_id:
            kwargs["MessageDeduplicationId"] = cmd.deduplication_id
        return kwargs

    @override
    async def publish(self, cmd: "SQSPublishCommand") -> Any:
        kwargs = await self._build_send_kwargs(cmd)
        return await self._connected_client.send_message(**kwargs)

    @override
    async def publish_batch(self, cmd: "SQSPublishCommand") -> Any:
        self._validate_fifo(cmd)
        url = await self.get_queue_url(cmd.queue)

        if isinstance(self.codec, BatchCodecProto):
            encoded_batch = await self.codec.encode_batch(
                cmd.batch_bodies, self.serializer
            )
        else:
            encoded_batch = [
                await self.codec.encode(body, self.serializer)
                for body in cmd.batch_bodies
            ]

        entries: list[dict[str, Any]] = []
        for idx, (payload, content_type) in enumerate(encoded_batch):
            text, attrs = self._build_entry(payload, content_type, cmd)
            entry: dict[str, Any] = {
                "Id": str(idx),
                "MessageBody": text,
                "MessageAttributes": attrs,
            }
            if cmd.group_id:
                entry["MessageGroupId"] = cmd.group_id
            if cmd.deduplication_id:
                entry["MessageDeduplicationId"] = f"{cmd.deduplication_id}-{idx}"
            entries.append(entry)

        # SQS caps a batch at 10 entries and 256 KiB total; split into chunks.
        successful: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for chunk in self._chunk_entries(entries):
            resp = await self._connected_client.send_message_batch(
                QueueUrl=url,
                Entries=chunk,  # type: ignore[arg-type]
            )
            successful.extend(cast("list[dict[str, Any]]", resp.get("Successful", [])))
            failed.extend(cast("list[dict[str, Any]]", resp.get("Failed", [])))

        if failed:
            raise BatchSendError(failed)

        return {"Successful": successful, "Failed": failed}

    @staticmethod
    def _chunk_entries(entries: list[dict[str, Any]]) -> Iterator[list[dict[str, Any]]]:
        """Yield batches of at most 10 entries, each under the 256 KiB limit."""
        chunk: list[dict[str, Any]] = []
        chunk_size = 0
        for entry in entries:
            entry_size = len(entry["MessageBody"].encode()) + (
                SQSFastProducer._attributes_size(entry.get("MessageAttributes", {}))
            )
            too_many = len(chunk) >= MAX_BATCH_ENTRIES
            too_big = chunk and (chunk_size + entry_size) > MAX_MESSAGE_SIZE
            if too_many or too_big:
                yield chunk
                chunk, chunk_size = [], 0
            chunk.append(entry)
            chunk_size += entry_size
        if chunk:
            yield chunk

    @override
    async def request(self, cmd: "SQSPublishCommand") -> Any:
        if self.response_queue_url is None:
            msg = (
                "SQS RPC requires a response queue. "
                "Pass `response_queue=...` to SQSBroker(...)."
            )
            raise FeatureNotSupportedException(msg)

        correlation_id = cmd.correlation_id or gen_cor_id()
        cmd.correlation_id = correlation_id
        cmd.reply_to = self.response_queue_url

        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._futures[correlation_id] = future

        try:
            await self.publish(cmd)
            with anyio.fail_after(cmd.timeout or 30.0):
                return await future
        finally:
            self._futures.pop(correlation_id, None)

    def resolve_response(self, correlation_id: str, raw_message: Any) -> bool:
        """Resolve a pending RPC future. Returns True if a waiter was found."""
        future = self._futures.get(correlation_id)
        if future is not None and not future.done():
            future.set_result(raw_message)
            return True
        return False
