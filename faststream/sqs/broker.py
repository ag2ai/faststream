import asyncio
import logging
from functools import partial, wraps
from types import TracebackType
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    NoReturn,
    Optional,
    Sequence,
    Type,
    Union,
)

import anyio
from aiobotocore.client import AioBaseClient
from aiobotocore.session import get_session
from fast_depends.dependencies import Depends

from faststream import BaseMiddleware
from faststream._compat import model_to_dict
from faststream.broker.core.asyncronous import BrokerAsyncUsecase, default_filter
from faststream.broker.message import StreamMessage
from faststream.broker.publisher import BasePublisher
from faststream.broker.push_back_watcher import BaseWatcher, WatcherContext
from faststream.broker.types import (
    AsyncPublisherProtocol,
    CustomDecoder,
    CustomParser,
    Filter,
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
    WrappedReturn,
)
from faststream.broker.wrapper import FakePublisher, HandlerCallWrapper
from faststream.sqs import SQSQueue
from faststream.sqs.asyncapi import Handler, Publisher
from faststream.sqs.handler import QueueUrl
from faststream.sqs.producer import SQSFastProducer
from faststream.sqs.shared.logging import SQSLoggingMixin
from faststream.types import AnyDict, SendableMessage
from faststream.utils import context


class SQSBroker(
    SQSLoggingMixin,
    BrokerAsyncUsecase[AnyDict, AioBaseClient],
):
    handlers: Dict[str, Handler]  # type: ignore[assignment]
    _publishers: Dict[str, Publisher]  # type: ignore[assignment]
    _producer: Optional[SQSFastProducer]

    def __init__(
        self,
        url: str = "http://localhost:9324/",
        *,
        log_fmt: Optional[str] = None,
        response_queue: str = "",
        protocol: str = "sqs",
        **kwargs: Any,
    ) -> None:
        super().__init__(
            url,
            log_fmt=log_fmt,
            url_=url,
            protocol=protocol,
            **kwargs,
        )
        self._queues = {}
        self.response_queue = response_queue
        self.response_callbacks = {}

    async def _connect(self, *, url: str, **kwargs: Any) -> AioBaseClient:
        session = get_session()
        client: AioBaseClient = await session._create_client(
            service_name="sqs", endpoint_url=url, **kwargs
        )
        context.set_global("client", client)
        await client.__aenter__()
        return client

    async def _close(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exec_tb: Optional[TracebackType] = None,
    ) -> None:
        await super().close(exc_type, exc_val, exec_tb)
        for f in self.response_callbacks.values():
            f.cancel()
        self.response_callbacks = {}

        for h in self.handlers:
            if h.task is not None:
                h.task.cancel()
                h.task = None

        if self._connection is not None:
            await self._connection.__aexit__(None, None, None)
            self._connection = None

    def _process_message(
        self,
        func: Callable[[StreamMessage[AnyDict]], Awaitable[T_HandlerReturn]],
        watcher: BaseWatcher,
    ) -> Callable[[StreamMessage[AnyDict]], Awaitable[WrappedReturn[T_HandlerReturn]],]:
        @wraps(func)
        async def process_wrapper(
            message: StreamMessage[AnyDict],
        ) -> WrappedReturn[T_HandlerReturn]:
            async with WatcherContext(watcher, message):
                r = await self._execute_handler(func, message)
                pub_response: Optional[AsyncPublisherProtocol]
                if message.reply_to:
                    pub_response = FakePublisher(
                        partial(self.publish, subject=message.reply_to)
                    )
                else:
                    pub_response = None

                return r, pub_response

        return process_wrapper

    async def create_queue(self, queue: SQSQueue) -> QueueUrl:
        url = self._queues.get(queue.name)
        if url is None:  # pragma: no branch
            url = (
                await self._connection.create_queue(
                    QueueName=queue.name,
                    Attributes={
                        i: str(j)
                        for i, j in model_to_dict(
                            queue,
                            exclude={"name", "tags"},
                            by_alias=True,
                            exclude_defaults=True,
                            exclude_unset=True,
                        ).items()
                    },
                    tags=queue.tags,
                )
            ).get("QueueUrl", "")
            self._queues[queue.name] = url
        return url

    async def start(self) -> None:
        context.set_local(
            "log_context",
            self._get_log_context(None, ""),
        )

        await super().start()

        for handler in self.handlers.values():  # pragma: no branch
            c = self._get_log_context(None, handler.queue.name)
            self._log(f"`{handler.call_name.__name__}` waiting for messages", extra=c)

            url = await self.create_queue(handler.queue)
            handler.task = asyncio.create_task(self._consume(url, handler))

    def subscriber(
        self,
        *broker_args: Any,
        retry: Union[bool, int] = False,
        dependencies: Sequence[Depends] = (),
        decoder: Optional[CustomDecoder[StreamMessage[MsgType]]] = None,
        parser: Optional[CustomParser[MsgType, StreamMessage[MsgType]]] = None,
        middlewares: Optional[Sequence[Callable[[MsgType], BaseMiddleware]]] = None,
        filter: Filter[StreamMessage[MsgType]] = default_filter,
        _raw: bool = False,
        _get_dependant: Optional[Any] = None,
        **broker_kwargs: Any,
    ) -> Callable[
        [
            Union[
                Callable[P_HandlerParams, T_HandlerReturn],
                HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn],
            ]
        ],
        HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn],
    ]:
        # TODO
        pass

    async def publish(
        self,
        message: SendableMessage,
        *args: Any,
        reply_to: str = "",
        rpc: bool = False,
        rpc_timeout: Optional[float] = None,
        raise_timeout: bool = False,
        **kwargs: Any,
    ) -> Optional[SendableMessage]:
        # TODO
        pass

    def publisher(
        self, key: Any, publisher: BasePublisher[MsgType]
    ) -> BasePublisher[MsgType]:
        # TODO
        pass

    async def _consume(self, queue_url: str, handler: Handler) -> NoReturn:
        c = self._get_log_context(None, handler.queue.name)

        connected = True
        with context.scope("queue_url", queue_url):
            while True:
                try:
                    if connected is False:
                        await self.create_queue(handler.queue)

                    r = await self._connection.receive_message(
                        QueueUrl=queue_url,
                        **handler.consumer_params,
                    )

                except Exception as e:
                    if connected is True:
                        self._log(e, logging.WARNING, c, exc_info=e)
                        self._queues.pop(handler.queue.name)
                        connected = False

                    await anyio.sleep(5)

                else:
                    if connected is False:
                        self._log("Connection established", logging.INFO, c)
                        connected = True

                    messages = r.get("Messages", [])
                    for msg in messages:
                        try:
                            await handler.callback(msg, True)
                        except Exception:
                            has_trash_messages = True
                        else:
                            has_trash_messages = False

                    if has_trash_messages is True:
                        await anyio.sleep(
                            handler.consumer_params.get("WaitTimeSeconds", 1.0)
                        )
