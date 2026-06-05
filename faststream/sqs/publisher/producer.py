import asyncio
from typing import TYPE_CHECKING, Any, Optional

import anyio
from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException, IncorrectState
from faststream.message import gen_cor_id
from faststream.sqs.parser import SQSParser
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
        resp = await self._connected_client.get_queue_url(QueueName=queue)
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

    async def _build_send_kwargs(self, cmd: "SQSPublishCommand") -> dict[str, Any]:
        payload, content_type = await self.codec.encode(cmd.body, self.serializer)
        body = payload.decode() if isinstance(payload, bytes) else str(payload)

        kwargs: dict[str, Any] = {
            "QueueUrl": await self.get_queue_url(cmd.queue),
            "MessageBody": body,
            "MessageAttributes": self._build_message_attributes(cmd, content_type),
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
        url = await self.get_queue_url(cmd.queue)
        entries: list[dict[str, Any]] = []
        for idx, body in enumerate(cmd.batch_bodies):
            payload, content_type = await self.codec.encode(body, self.serializer)
            text = payload.decode() if isinstance(payload, bytes) else str(payload)
            entry: dict[str, Any] = {
                "Id": str(idx),
                "MessageBody": text,
                "MessageAttributes": self._build_message_attributes(cmd, content_type),
            }
            if cmd.group_id:
                entry["MessageGroupId"] = cmd.group_id
            if cmd.deduplication_id:
                entry["MessageDeduplicationId"] = f"{cmd.deduplication_id}-{idx}"
            entries.append(entry)

        return await self._connected_client.send_message_batch(
            QueueUrl=url,
            Entries=entries,  # type: ignore[arg-type]
        )

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
