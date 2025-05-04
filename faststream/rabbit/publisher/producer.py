from typing import (
    TYPE_CHECKING,
    Optional,
    Protocol,
    cast,
)

import anyio
from typing_extensions import Unpack, override

from faststream._internal.publisher.proto import ProducerProto
from faststream._internal.subscriber.utils import resolve_custom_func
from faststream.exceptions import FeatureNotSupportedException, IncorrectState
from faststream.rabbit.parser import AioPikaParser
from faststream.rabbit.schemas import RABBIT_REPLY, RabbitExchange

if TYPE_CHECKING:
    from types import TracebackType

    import aiormq
    from aio_pika import IncomingMessage, RobustQueue
    from aio_pika.abc import AbstractIncomingMessage, TimeoutType
    from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

    from faststream._internal.types import (
        AsyncCallable,
        CustomCallable,
    )
    from faststream.rabbit.helpers import RabbitDeclarer
    from faststream.rabbit.response import MessageOptions, RabbitPublishCommand
    from faststream.rabbit.types import AioPikaSendableMessage


class LockState(Protocol):
    lock: "anyio.Lock"


class LockUnset(LockState):
    __slots__ = ()

    @property
    def lock(self) -> "anyio.Lock":
        msg = "You should call `producer.connect()` method at first."
        raise IncorrectState(msg)


class RealLock(LockState):
    __slots__ = ("lock",)

    def __init__(self) -> None:
        self.lock = anyio.Lock()


class AioPikaFastProducer(ProducerProto):
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

        default_parser = AioPikaParser()
        self._parser = resolve_custom_func(parser, default_parser.parse_message)
        self._decoder = resolve_custom_func(decoder, default_parser.decode_message)

    def connect(self) -> None:
        """Lock initialization.

        Should be called in async context due `anyio.Lock` object can't be created outside event loop.
        """
        self.__lock = RealLock()

    def disconnect(self) -> None:
        self.__lock = LockUnset()

    @override
    async def publish(  # type: ignore[override]
        self,
        cmd: "RabbitPublishCommand",
    ) -> Optional["aiormq.abc.ConfirmationFrameType"]:
        """Publish a message to a RabbitMQ queue."""
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
    async def request(  # type: ignore[override]
        self,
        cmd: "RabbitPublishCommand",
    ) -> "IncomingMessage":
        """Publish a message to a RabbitMQ queue."""
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
        """Publish a message to a RabbitMQ exchange."""
        message = AioPikaParser.encode_message(message=message, **message_options)

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

    @override
    async def publish_batch(
        self,
        cmd: "RabbitPublishCommand",
    ) -> None:
        msg = "RabbitMQ doesn't support publishing in batches."
        raise FeatureNotSupportedException(msg)


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
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        self.lock.release()
        await self.queue.cancel(self.consumer_tag)
