from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
    Optional,
    Protocol,
    cast,
)

import anyio
from typing_extensions import Unpack, override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException, IncorrectState
from faststream.rabbit.parser import AioPikaParser
from faststream.rabbit.response import RabbitPublishCommand
from faststream.rabbit.schemas import RABBIT_REPLY, RabbitExchange

if TYPE_CHECKING:
    from types import TracebackType

    import aiormq
    from aio_pika import IncomingMessage, RobustQueue
    from aio_pika.abc import AbstractIncomingMessage, TimeoutType
    from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.types import (
        AsyncCallable,
        CustomCallable,
    )
    from faststream.rabbit.helpers import RabbitDeclarer
    from faststream.rabbit.types import AioPikaSendableMessage

    from .options import MessageOptions


class LockState(Protocol):
    @property
    def lock(self) -> "anyio.Lock": ...


class LockUnset:
    __slots__ = ()

    @property
    def lock(self) -> "anyio.Lock":
        msg = "You should call `producer.connect()` method at first."
        raise IncorrectState(msg)


class RealLock:
    __slots__ = ("lock",)

    def __init__(self) -> None:
        self.lock = anyio.Lock()


class AioPikaFastProducer(ProducerProto[RabbitPublishCommand]):
    def connect(self, serializer: Optional["SerializerProto"] = None) -> None: ...

    def disconnect(self) -> None: ...

    @abstractmethod
    async def publish(
        self,
        cmd: "RabbitPublishCommand",
    ) -> Optional["aiormq.abc.ConfirmationFrameType"]: ...

    @abstractmethod
    async def request(self, cmd: "RabbitPublishCommand") -> "IncomingMessage": ...

    @override
    async def publish_batch(self, cmd: "RabbitPublishCommand") -> None:
        msg = "RabbitMQ doesn't support publishing in batches."
        raise FeatureNotSupportedException(msg)


class FakeAioPikaFastProducer(AioPikaFastProducer):
    def __bool__(self) -> bool:
        return False

    def connect(self, serializer: Optional["SerializerProto"] = None) -> None:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError

    @override
    async def publish(
        self,
        cmd: "RabbitPublishCommand",
    ) -> Optional["aiormq.abc.ConfirmationFrameType"]:
        raise NotImplementedError

    @override
    async def request(self, cmd: "RabbitPublishCommand") -> "IncomingMessage":
        raise NotImplementedError


class AioPikaFastProducerImpl(AioPikaFastProducer):
    """A class for fast producing messages using aio-pika."""

    _decoder: "AsyncCallable"
    _parser: "AsyncCallable"

    def __init__(
        self,
        *,
        declarer: "RabbitDeclarer",
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self.declarer = declarer

        self.__lock: LockState = LockUnset()
        self.serializer: SerializerProto | None = None

        default_parser = AioPikaParser()
        self._parser = ParserComposition(parser, default_parser.parse_message)
        self._decoder = ParserComposition(decoder, default_parser.decode_message)

    def connect(self, serializer: Optional["SerializerProto"] = None) -> None:
        """Lock initialization.

        Should be called in async context due `anyio.Lock` object can't be created outside event loop.
        """
        self.serializer = serializer
        self.__lock = RealLock()

    def disconnect(self) -> None:
        self.__lock = LockUnset()

    @override
    async def publish(
        self,
        cmd: "RabbitPublishCommand",
    ) -> Optional["aiormq.abc.ConfirmationFrameType"]:
        return await self._publish(
            message=cmd.body,
            exchange=cmd.exchange,
            routing_key=cmd.destination,
            reply_to=cmd.reply_to,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            **cmd.publish_options,
            **cmd.message_options,
        )

    @override
    async def request(self, cmd: "RabbitPublishCommand") -> "IncomingMessage":
        async with _RPCCallback(
            self.__lock.lock,
            await self.declarer.declare_queue(RABBIT_REPLY),
        ) as response_queue:
            with anyio.fail_after(cmd.timeout):
                await self._publish(
                    message=cmd.body,
                    exchange=cmd.exchange,
                    routing_key=cmd.destination,
                    reply_to=RABBIT_REPLY.name,
                    headers=cmd.headers,
                    correlation_id=cmd.correlation_id,
                    **cmd.publish_options,
                    **cmd.message_options,
                )
                return await response_queue.receive()

    async def _publish(
        self,
        message: "AioPikaSendableMessage",
        *,
        exchange: "RabbitExchange",
        routing_key: str,
        mandatory: bool = True,
        immediate: bool = False,
        timeout: "TimeoutType" = None,
        **message_options: Unpack["MessageOptions"],
    ) -> Optional["aiormq.abc.ConfirmationFrameType"]:
        message = AioPikaParser.encode_message(
            message=message, serializer=self.serializer, **message_options
        )

        exchange_obj = await self.declarer.declare_exchange(
            exchange=exchange,
            declare=False,
        )

        return await exchange_obj.publish(
            message=message,
            routing_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout,
        )


class _RPCCallback:
    """A class provides an RPC lock."""

    def __init__(self, lock: "anyio.Lock", callback_queue: "RobustQueue") -> None:
        self.lock = lock
        self.queue = callback_queue

    async def __aenter__(self) -> "MemoryObjectReceiveStream[IncomingMessage]":
        send_response_stream: MemoryObjectSendStream[AbstractIncomingMessage]
        receive_response_stream: MemoryObjectReceiveStream[AbstractIncomingMessage]

        (
            send_response_stream,
            receive_response_stream,
        ) = anyio.create_memory_object_stream(max_buffer_size=1)
        await self.lock.acquire()

        self.consumer_tag = await self.queue.consume(
            callback=send_response_stream.send,
            no_ack=True,
        )

        return cast(
            "MemoryObjectReceiveStream[IncomingMessage]",
            receive_response_stream,
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        self.lock.release()
        await self.queue.cancel(self.consumer_tag)
