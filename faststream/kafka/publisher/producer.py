from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import BatchCodecProto, DefaultCodec
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException
from faststream.kafka.exceptions import BatchBufferOverflowException
from faststream.kafka.message import KafkaMessage
from faststream.kafka.parser import AioKafkaParser
from faststream.kafka.response import KafkaPublishCommand
from faststream.message import TOMBSTONE, Tombstone
from faststream.message.utils import encode_or_tombstone, ensure_tombstone_key

from .state import EmptyProducerState, ProducerState, RealProducer

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Sequence

    from aiokafka import AIOKafkaProducer
    from aiokafka.structs import RecordMetadata
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.parser import CodecProto
    from faststream._internal.types import CustomCallable


class AioKafkaFastProducer(ProducerProto[KafkaPublishCommand]):
    async def connect(
        self,
        producer: "AIOKafkaProducer",
        serializer: Optional["SerializerProto"],
        codec: Optional["CodecProto"] = None,
    ) -> None: ...

    async def disconnect(self) -> None: ...

    def __bool__(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return True

    async def flush(self) -> None:
        return None

    @abstractmethod
    async def publish(
        self,
        cmd: "KafkaPublishCommand",
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]: ...

    @abstractmethod
    async def publish_batch(
        self,
        cmd: "KafkaPublishCommand",
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]: ...

    async def request(self, cmd: "KafkaPublishCommand") -> Any:
        msg = "Kafka doesn't support `request` method without test client."
        raise FeatureNotSupportedException(msg)


class AioKafkaFastProducerImpl(AioKafkaFastProducer):
    """A class to represent Kafka producer."""

    def __init__(
        self,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self._producer: ProducerState = EmptyProducerState()
        self.serializer: SerializerProto | None = None
        self.codec: CodecProto = DefaultCodec()

        # NOTE: register default parser to be compatible with request
        default = AioKafkaParser(msg_class=KafkaMessage, regex=None)
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)

    async def connect(
        self,
        producer: "AIOKafkaProducer",
        serializer: Optional["SerializerProto"],
        codec: Optional["CodecProto"] = None,
    ) -> None:
        self.serializer = serializer
        self.codec = codec or DefaultCodec()
        await producer.start()
        self._producer = RealProducer(producer)

    async def disconnect(self) -> None:
        await self._producer.stop()
        self._producer = EmptyProducerState()

    def __bool__(self) -> bool:
        return bool(self._producer)

    @property
    def closed(self) -> bool:
        return self._producer.closed

    async def flush(self) -> None:
        await self._producer.flush()

    @override
    async def publish(
        self,
        cmd: "KafkaPublishCommand",
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]:
        """Publish a message to a topic."""
        if cmd.body is TOMBSTONE:
            ensure_tombstone_key(cmd.key)
        message, content_type = await encode_or_tombstone(
            cmd.body, self.codec, self.serializer
        )

        headers_to_send = {
            "content-type": content_type or "",
            **cmd.headers_to_publish(),
        }

        send_future = await self._producer.producer.send(
            topic=cmd.destination,
            value=message,
            key=cmd.key,
            partition=cmd.partition,
            timestamp_ms=cmd.timestamp_ms,
            headers=[(i, (j or "").encode()) for i, j in headers_to_send.items()],
        )

        if not cmd.no_confirm:
            return await send_future
        return send_future

    @override
    async def publish_batch(
        self,
        cmd: "KafkaPublishCommand",
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]:
        """Publish a batch of messages to a topic."""
        batch = self._producer.producer.create_batch()

        headers_to_send = cmd.headers_to_publish()

        encoded_batch: Sequence[tuple[bytes | None, str | None]]
        if isinstance(self.codec, BatchCodecProto):
            if any(isinstance(body, Tombstone) for body in cmd.batch_bodies):
                msg = (
                    "a tombstone in a batch isn't supported with a custom BatchCodecProto"
                )
                raise ValueError(msg)
            encoded_batch = await self.codec.encode_batch(
                cmd.batch_bodies, self.serializer
            )
        else:
            for message_position, body in enumerate(cmd.batch_bodies):
                if body is TOMBSTONE:
                    ensure_tombstone_key(cmd.key_for(message_position))
            encoded_batch = [
                await encode_or_tombstone(body, self.codec, self.serializer)
                for body in cmd.batch_bodies
            ]

        for message_position, (message, content_type) in enumerate(encoded_batch):
            if content_type:
                final_headers = {
                    "content-type": content_type,
                    **headers_to_send,
                }
            else:
                final_headers = headers_to_send.copy()

            metadata = batch.append(
                key=cmd.key_for(message_position),
                value=message,
                timestamp=cmd.timestamp_ms,
                headers=[(i, j.encode()) for i, j in final_headers.items()],
            )
            if metadata is None:
                raise BatchBufferOverflowException(message_position=message_position)

        send_future = await self._producer.producer.send_batch(
            batch,
            cmd.destination,
            partition=cmd.partition,
        )
        if not cmd.no_confirm:
            return await send_future
        return send_future


class FakeAioKafkaFastProducer(AioKafkaFastProducer):
    async def connect(
        self,
        producer: "AIOKafkaProducer",
        serializer: Optional["SerializerProto"],
        codec: Optional["CodecProto"] = None,
    ) -> None:
        raise NotImplementedError

    async def disconnect(self) -> None:
        raise NotImplementedError

    def __bool__(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        raise NotImplementedError

    async def flush(self) -> None:
        raise NotImplementedError

    async def publish(
        self,
        cmd: "KafkaPublishCommand",
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]:
        raise NotImplementedError

    async def publish_batch(
        self,
        cmd: "KafkaPublishCommand",
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]:
        raise NotImplementedError
