from abc import abstractmethod
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from contextlib import AbstractContextManager, AsyncExitStack
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Generic,
    NamedTuple,
    Optional,
    Union,
)

from typing_extensions import Self, deprecated, overload, override

from faststream._internal.endpoint.usecase import Endpoint
from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.types import (
    AsyncCallable,
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream._internal.utils.functions import FakeContext, to_async
from faststream.exceptions import StopConsume, SubscriberNotFound
from faststream.middlewares import AcknowledgementMiddleware
from faststream.middlewares.logging import CriticalLogMiddleware
from faststream.response import ensure_response

from .call_item import (
    CallsCollection,
    HandlerItem,
)
from .utils import MultiLock, default_filter

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import Decorator
    from faststream._internal.configs import SubscriberUsecaseConfig
    from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.types import (
        AsyncFilter,
        BrokerMiddleware,
        CustomCallable,
        Filter,
        SubscriberMiddleware,
    )
    from faststream.message import StreamMessage
    from faststream.middlewares import BaseMiddleware
    from faststream.response import Response
    from faststream.specification.schema import SubscriberSpec

    from .specification import SubscriberSpecification


class _CallOptions(NamedTuple):
    parser: Optional["CustomCallable"]
    decoder: Optional["CustomCallable"]
    middlewares: Sequence["SubscriberMiddleware[Any]"]
    dependencies: Iterable["Dependant"]


class SubscriberUsecase(Endpoint, Generic[MsgType]):
    """A class representing an asynchronous handler."""

    lock: "AbstractContextManager[Any]"
    extra_watcher_options: dict[str, Any]
    graceful_timeout: float | None

    def __init__(
        self,
        config: "SubscriberUsecaseConfig",
        specification: "SubscriberSpecification",
        calls: "CallsCollection[MsgType]",
    ) -> None:
        """Initialize a new instance of the class."""
        super().__init__(config._outer_config)

        self.calls = calls
        self.specification = specification

        self._no_reply = config.no_reply
        self._parser = config.parser
        self._decoder = config.decoder

        self.ack_policy = config.ack_policy
        self.__auto_ack_disabled = config.auto_ack_disabled

        self._call_options = _CallOptions(
            parser=None,
            decoder=None,
            middlewares=(),
            dependencies=(),
        )

        self._call_decorators: tuple[Decorator, ...] = ()

        self.running = False
        self.lock = FakeContext()

        self.extra_watcher_options = {}

    @property
    def _broker_middlewares(self) -> Sequence["BrokerMiddleware[MsgType]"]:
        return self._outer_config.broker_middlewares

    async def start(self) -> None:
        """Private method to start subscriber by broker."""
        self.lock = MultiLock()

        self._build_fastdepends_model()

        self._outer_config.logger.log(
            f"`{self.specification.call_name}` waiting for messages",
            extra=self.get_log_context(None),
        )

    def _get_parser_and_decoder(
        self,
        item_parser: Optional["CustomCallable"] = None,
        item_decoder: Optional["CustomCallable"] = None,
    ) -> tuple[AsyncCallable, AsyncCallable]:
        """Method to resolve parsers with priority.

        First priority
        >>> sub = broker.subscriber()
        >>>
        >>> @sub(parser=P0_parser)
        >>> async def handler(): ...

        Second priority
        >>> sub = broker.subscriber(parser=P1_parser)

        Third priority
        >>> Broker(parser=P2_parser)

        Default parser is `self._parser`.
        So, the final parser object is
        >>> ParserComposition(P0_parser or P1_parser or P2_parser, self._parser)
        """
        if parser := (
            item_parser or self._call_options.parser or self._outer_config.broker_parser
        ):
            async_parser: AsyncCallable = ParserComposition(parser, self._parser)
        else:
            async_parser = self._parser

        if decoder := (
            item_decoder
            or self._call_options.decoder
            or self._outer_config.broker_decoder
        ):
            async_decoder: AsyncCallable = ParserComposition(decoder, self._decoder)
        else:
            async_decoder = self._decoder

        return async_parser, async_decoder

    def _build_fastdepends_model(self) -> None:
        for call in self.calls:
            async_parser, async_decoder = self._get_parser_and_decoder(
                call.item_parser, call.item_decoder
            )

            call._setup(
                parser=async_parser,
                decoder=async_decoder,
                config=self._outer_config.fd_config,
                broker_dependencies=self._outer_config.broker_dependencies,
                _call_decorators=self._call_decorators,
            )

            call.handler.refresh(with_mock=False)

    def _post_start(self) -> None:
        self.running = True

    @abstractmethod
    async def stop(self) -> None:
        """Stop message consuming.

        Blocks event loop up to graceful_timeout seconds.
        """
        # set running false before releasing to stop new message reading
        self.running = False

        # Wait for already consumed messages to be processed
        if isinstance(self.lock, MultiLock):
            await self.lock.wait_release(self._outer_config.graceful_timeout)

    def add_call(
        self,
        *,
        parser_: Optional["CustomCallable"],
        decoder_: Optional["CustomCallable"],
        middlewares_: Sequence["SubscriberMiddleware[Any]"],
        dependencies_: Iterable["Dependant"],
    ) -> Self:
        self._call_options = _CallOptions(
            parser=parser_,
            decoder=decoder_,
            middlewares=middlewares_,
            dependencies=dependencies_,
        )
        return self

    @overload
    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn],
        *,
        filter: "Filter[Any]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        dependencies: Iterable["Dependant"] = (),
    ) -> "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]": ...

    @overload
    def __call__(
        self,
        func: None = None,
        *,
        filter: "Filter[Any]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        dependencies: Iterable["Dependant"] = (),
    ) -> Callable[
        [Callable[P_HandlerParams, T_HandlerReturn]],
        "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]",
    ]: ...

    @override
    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn] | None = None,
        *,
        filter: "Filter[Any]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        dependencies: Iterable["Dependant"] = (),
    ) -> Union[
        "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]",
        Callable[
            [Callable[P_HandlerParams, T_HandlerReturn]],
            "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]",
        ],
    ]:
        total_deps = (*self._call_options.dependencies, *dependencies)
        total_middlewares = (*self._call_options.middlewares, *middlewares)
        async_filter: AsyncFilter[StreamMessage[MsgType]] = to_async(filter)

        def real_wrapper(
            func: Callable[P_HandlerParams, T_HandlerReturn],
        ) -> "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]":
            handler = super(SubscriberUsecase, self).__call__(func)
            handler._subscribers.append(self)

            self.calls.add_call(
                HandlerItem[MsgType](
                    handler=handler,
                    filter=async_filter,
                    item_parser=parser,
                    item_decoder=decoder,
                    item_middlewares=total_middlewares,
                    dependencies=total_deps,
                ),
            )

            return handler

        if func is None:
            return real_wrapper

        return real_wrapper(func)

    async def consume(self, msg: MsgType) -> Any:
        """Consume a message asynchronously."""
        if not self.running:
            return None

        try:
            return await self.process_message(msg)

        except StopConsume:
            # Stop handler at StopConsume exception
            await self.stop()

        except SystemExit:
            # Stop handler at `exit()` call
            await self.stop()

            if app := self._outer_config.fd_config.context.get("app"):
                app.exit()

        except Exception:  # nosec B110
            # All other exceptions were logged by CriticalLogMiddleware
            pass

    async def process_message(self, msg: MsgType) -> "Response":
        """Execute all message processing stages."""
        context = self._outer_config.fd_config.context
        logger_state = self._outer_config.logger

        async with AsyncExitStack() as stack:
            stack.enter_context(self.lock)

            # Enter context before middlewares
            stack.enter_context(context.scope("handler_", self))
            stack.enter_context(context.scope("logger", logger_state.logger.logger))
            for k, v in self._outer_config.extra_context.items():
                stack.enter_context(context.scope(k, v))

            # enter all middlewares
            middlewares: list[BaseMiddleware] = []
            for base_m in self.__build__middlewares_stack():
                middleware = base_m(msg, context=context)
                middlewares.append(middleware)
                await middleware.__aenter__()

            cache: dict[Any, Any] = {}
            parsing_error: Exception | None = None
            for h in self.calls:
                try:
                    message = await h.is_suitable(msg, cache)
                except Exception as e:
                    parsing_error = e
                    break

                if message is not None:
                    stack.enter_context(
                        context.scope("log_context", self.get_log_context(message)),
                    )
                    stack.enter_context(context.scope("message", message))

                    # Middlewares should be exited before scope release
                    for m in middlewares:
                        stack.push_async_exit(m.__aexit__)

                    result_msg = ensure_response(
                        await h.call(
                            message=message,
                            # consumer middlewares
                            _extra_middlewares=(
                                m.consume_scope for m in middlewares[::-1]
                            ),
                        ),
                    )

                    if not result_msg.correlation_id:
                        result_msg.correlation_id = message.correlation_id

                    for p in chain(
                        self.__get_response_publisher(message),
                        h.handler._publishers,
                    ):
                        await p._publish(
                            result_msg.as_publish_command(),
                            _extra_middlewares=(
                                m.publish_scope for m in middlewares[::-1]
                            ),
                        )

                    # Return data for tests
                    return result_msg

            # Suitable handler was not found or
            # parsing/decoding exception occurred
            for m in middlewares:
                stack.push_async_exit(m.__aexit__)

            # Reraise it to catch in tests
            if parsing_error:
                raise parsing_error

            error_msg = f"There is no suitable handler for {msg=}"
            raise SubscriberNotFound(error_msg)

        # An error was raised and processed by some middleware
        return ensure_response(None)

    def __build__middlewares_stack(self) -> tuple["BrokerMiddleware[MsgType]", ...]:
        logger_state = self._outer_config.logger

        if self.__auto_ack_disabled:
            broker_middlewares = (
                CriticalLogMiddleware(logger_state),
                *self._broker_middlewares,
            )

        else:
            broker_middlewares = (
                AcknowledgementMiddleware(
                    logger=logger_state,
                    ack_policy=self.ack_policy,
                    extra_options=self.extra_watcher_options,
                ),
                CriticalLogMiddleware(logger_state),
                *self._broker_middlewares,
            )

        return broker_middlewares

    def __get_response_publisher(
        self,
        message: "StreamMessage[MsgType]",
    ) -> Iterable["PublisherProto"]:
        if not message.reply_to or self._no_reply:
            return ()

        return self._make_response_publisher(message)

    @abstractmethod
    def _make_response_publisher(
        self,
        message: "StreamMessage[MsgType]",
    ) -> Iterable["PublisherProto"]:
        raise NotImplementedError

    @abstractmethod
    async def get_one(self, *, timeout: float = 5) -> Optional["StreamMessage[MsgType]"]:
        raise NotImplementedError

    @abstractmethod
    async def __aiter__(self) -> AsyncIterator["StreamMessage[MsgType]"]:
        raise NotImplementedError

    def get_log_context(
        self,
        message: Optional["StreamMessage[MsgType]"],
    ) -> dict[str, str]:
        """Generate log context."""
        return {
            "message_id": getattr(message, "message_id", ""),
        }

    def _log(
        self,
        log_level: int | None,
        message: str,
        extra: dict[str, Any] | None = None,
        exc_info: Exception | None = None,
    ) -> None:
        self._outer_config.logger.log(
            message,
            log_level,
            extra=extra,
            exc_info=exc_info,
        )

    def schema(self) -> dict[str, "SubscriberSpec"]:
        self._build_fastdepends_model()
        return self.specification.get_schema()
