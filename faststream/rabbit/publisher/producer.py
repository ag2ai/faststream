import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    Optional,
    Type,
    Union,
    cast,
)

import anyio
from typing_extensions import override

from faststream.broker.publisher.proto import ProducerProto
from faststream.broker.utils import resolve_custom_func
from faststream.exceptions import WRONG_PUBLISH_ARGS
from faststream.rabbit.parser import AioPikaParser
from faststream.rabbit.schemas import RABBIT_REPLY, RabbitExchange
from faststream.utils.functions import fake_context, timeout_scope

if TYPE_CHECKING:
    from types import TracebackType

    import aiormq
    from aio_pika import IncomingMessage, RobustQueue
    from aio_pika.abc import AbstractIncomingMessage, DateType, HeadersType, TimeoutType
    from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

    from faststream.broker.types import (
        AsyncCallable,
        CustomCallable,
    )
    from faststream.rabbit.helpers import RabbitDeclarer
    from faststream.rabbit.types import AioPikaSendableMessage
    from faststream.types import SendableMessage


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

        self._rpc_lock = anyio.Lock()

        default_parser = AioPikaParser()
        self._parser = resolve_custom_func(parser, default_parser.parse_message)
        self._decoder = resolve_custom_func(decoder, default_parser.decode_message)

    @override
    async def publish(  # type: ignore[override]
        self,
        message: "AioPikaSendableMessage",
        exchange: Union["RabbitExchange", str, None] = None,
        *,
        correlation_id: str = "",
        routing_key: str = "",
        mandatory: bool = True,
        immediate: bool = False,
        timeout: "TimeoutType" = None,
        rpc: bool = False,
        rpc_timeout: Optional[float] = 30.0,
        raise_timeout: bool = False,
        persist: bool = False,
        reply_to: Optional[str] = None,
        headers: Optional["HeadersType"] = None,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        priority: Optional[int] = None,
        expiration: Optional["DateType"] = None,
        message_id: Optional[str] = None,
        timestamp: Optional["DateType"] = None,
        message_type: Optional[str] = None,
        user_id: Optional[str] = None,
        app_id: Optional[str] = None,
    ) -> Optional[Any]:
        """Publish a message to a RabbitMQ queue."""
        context: AsyncContextManager[
            Optional[MemoryObjectReceiveStream[IncomingMessage]]
        ]
        if rpc:
            if reply_to is not None:
                raise WRONG_PUBLISH_ARGS

            context = _RPCCallback(
                self._rpc_lock,
                await self.declarer.declare_queue(RABBIT_REPLY),
            )
        else:
            context = fake_context()

        async with context as response_queue:
            r = await self._publish(
                message=message,
                exchange=exchange,
                routing_key=routing_key,
                mandatory=mandatory,
                immediate=immediate,
                timeout=timeout,
                persist=persist,
                reply_to=reply_to if response_queue is None else RABBIT_REPLY.name,
                headers=headers,
                content_type=content_type,
                content_encoding=content_encoding,
                priority=priority,
                correlation_id=correlation_id,
                expiration=expiration,
                message_id=message_id,
                timestamp=timestamp,
                message_type=message_type,
                user_id=user_id,
                app_id=app_id,
            )

            if response_queue is None:
                return r

            else:
                msg: Optional[IncomingMessage] = None
                with timeout_scope(rpc_timeout, raise_timeout):
                    msg = await response_queue.receive()

                if msg:  # pragma: no branch
                    return await self._decoder(await self._parser(msg))

        return None

    @override
    async def request(  # type: ignore[override]
        self,
        message: "AioPikaSendableMessage",
        exchange: Union["RabbitExchange", str, None] = None,
        *,
        correlation_id: str = "",
        routing_key: str = "",
        mandatory: bool = True,
        immediate: bool = False,
        timeout: Optional[float] = None,
        persist: bool = False,
        headers: Optional["HeadersType"] = None,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        priority: Optional[int] = None,
        expiration: Optional["DateType"] = None,
        message_id: Optional[str] = None,
        timestamp: Optional["DateType"] = None,
        message_type: Optional[str] = None,
        user_id: Optional[str] = None,
        app_id: Optional[str] = None,
    ) -> "IncomingMessage":
        """Publish a message to a RabbitMQ queue."""
        async with _RPCCallback(
            self._rpc_lock,
            await self.declarer.declare_queue(RABBIT_REPLY),
        ) as response_queue:
            with anyio.fail_after(timeout):
                await self._publish(
                    message=message,
                    exchange=exchange,
                    routing_key=routing_key,
                    mandatory=mandatory,
                    immediate=immediate,
                    timeout=timeout,
                    persist=persist,
                    reply_to=RABBIT_REPLY.name,
                    headers=headers,
                    content_type=content_type,
                    content_encoding=content_encoding,
                    priority=priority,
                    correlation_id=correlation_id,
                    expiration=expiration,
                    message_id=message_id,
                    timestamp=timestamp,
                    message_type=message_type,
                    user_id=user_id,
                    app_id=app_id,
                )
                return await response_queue.receive()

    async def _publish(
        self,
        message: "AioPikaSendableMessage",
        *,
        correlation_id: str,
        exchange: Union["RabbitExchange", str, None],
        routing_key: str,
        mandatory: bool,
        immediate: bool,
        timeout: "TimeoutType",
        persist: bool,
        reply_to: Optional[str],
        headers: Optional["HeadersType"],
        content_type: Optional[str],
        content_encoding: Optional[str],
        priority: Optional[int],
        expiration: Optional["DateType"],
        message_id: Optional[str],
        timestamp: Optional["DateType"],
        message_type: Optional[str],
        user_id: Optional[str],
        app_id: Optional[str],
    ) -> Union["aiormq.abc.ConfirmationFrameType", "SendableMessage"]:
        """Publish a message to a RabbitMQ exchange."""
        if timestamp is None:
            timestamp = datetime.datetime.now(tz=datetime.timezone.utc)

        message = AioPikaParser.encode_message(
            message=message,
            persist=persist,
            reply_to=reply_to,
            headers=headers,
            content_type=content_type,
            content_encoding=content_encoding,
            priority=priority,
            correlation_id=correlation_id,
            expiration=expiration,
            message_id=message_id,
            timestamp=timestamp,
            message_type=message_type,
            user_id=user_id,
            app_id=app_id,
        )

        exchange_obj = await self.declarer.declare_exchange(
            exchange=RabbitExchange.validate(exchange),
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
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        self.lock.release()
        await self.queue.cancel(self.consumer_tag)
