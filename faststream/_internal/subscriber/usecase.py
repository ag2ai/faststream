from abc import abstractmethod
from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager, AsyncExitStack
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    NamedTuple,
    Optional,
    Union,
)

from typing_extensions import Self, deprecated, overload, override

from faststream._internal.subscriber.call_item import HandlerItem
from faststream._internal.subscriber.proto import SubscriberProto
from faststream._internal.subscriber.utils import (
    MultiLock,
    default_filter,
    resolve_custom_func,
)
from faststream._internal.types import (
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream._internal.utils.functions import sync_fake_context, to_async
from faststream.exceptions import SetupError, StopConsume, SubscriberNotFound
from faststream.middlewares import AckPolicy, AcknowledgementMiddleware
from faststream.middlewares.logging import CriticalLogMiddleware
from faststream.response import ensure_response

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict, Decorator
    from faststream._internal.context.repository import ContextRepo
    from faststream._internal.publisher.proto import (
        BasePublisherProto,
    )
    from faststream._internal.state import BrokerState, Pointer
    from faststream._internal.subscriber.call_wrapper import HandlerCallWrapper
    from faststream._internal.types import (
        AsyncCallable,
        AsyncFilter,
        BrokerMiddleware,
        CustomCallable,
        Filter,
        SubscriberMiddleware,
    )
    from faststream.message import StreamMessage
    from faststream.middlewares import BaseMiddleware
    from faststream.response import Response


class _CallOptions(NamedTuple):
    parser: Optional["CustomCallable"]
    decoder: Optional["CustomCallable"]
    middlewares: Sequence["SubscriberMiddleware[Any]"]
    dependencies: Iterable["Dependant"]


class SubscriberUsecase(SubscriberProto[MsgType]):
    """A class representing an asynchronous handler."""

    lock: "AbstractContextManager[Any]"
    extra_watcher_options: "AnyDict"
    extra_context: "AnyDict"
    graceful_timeout: Optional[float]

    _broker_dependencies: Iterable["Dependant"]
    _call_options: Optional["_CallOptions"]
    _call_decorators: Iterable["Decorator"]

    def __init__(
        self,
        *,
        no_reply: bool,
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Sequence["BrokerMiddleware[MsgType]"],
        default_parser: "AsyncCallable",
        default_decoder: "AsyncCallable",
        ack_policy: AckPolicy,
    ) -> None:
        """Initialize a new instance of the class."""
        self.calls = []

        self._parser = default_parser
        self._decoder = default_decoder
        self._no_reply = no_reply
        self.ack_policy = ack_policy

        self._call_options = None
        self._call_decorators = ()

        self.running = False
        self.lock = sync_fake_context()

        # Setup in include
        self._broker_dependencies = broker_dependencies
        self._broker_middlewares = broker_middlewares

        # register in setup later
        self.extra_context = {}
        self.extra_watcher_options = {}

    def add_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None:
        self._broker_middlewares = (*self._broker_middlewares, middleware)

    @override
    def _setup(
        self,
        *,
        extra_context: "AnyDict",
        # broker options
        broker_parser: Optional["CustomCallable"],
        broker_decoder: Optional["CustomCallable"],
        # dependant args
        state: "Pointer[BrokerState]",
    ) -> None:
        # TODO: add EmptyBrokerState to init
        self._state = state

        self.extra_context = extra_context

        for call in self.calls:
            if parser := call.item_parser or broker_parser:
                async_parser = resolve_custom_func(to_async(parser), self._parser)
            else:
                async_parser = self._parser

            if decoder := call.item_decoder or broker_decoder:
                async_decoder = resolve_custom_func(to_async(decoder), self._decoder)
            else:
                async_decoder = self._decoder

            self._parser = async_parser
            self._decoder = async_decoder

            call._setup(
                parser=async_parser,
                decoder=async_decoder,
                state=state,
                broker_dependencies=self._broker_dependencies,
                _call_decorators=self._call_decorators,
            )

            call.handler.refresh(with_mock=False)

    @abstractmethod
    async def start(self) -> None:
        """Start the handler."""
        self.lock = MultiLock()

        self.running = True

    @abstractmethod
    async def close(self) -> None:
        """Close the handler.

        Blocks event loop up to graceful_timeout seconds.
        """
        self.running = False
        if isinstance(self.lock, MultiLock):
            await self.lock.wait_release(self._state.get().graceful_timeout)

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
        func: None = None,
        *,
        filter: "Filter[StreamMessage[MsgType]]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0"
            ),
        ] = (),
        dependencies: Iterable["Dependant"] = (),
    ) -> Callable[
        [Callable[P_HandlerParams, T_HandlerReturn]],
        "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]",
    ]: ...

    @overload
    def __call__(
        self,
        func: Union[
            Callable[P_HandlerParams, T_HandlerReturn],
            "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]",
        ],
        *,
        filter: "Filter[StreamMessage[MsgType]]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0"
            ),
        ] = (),
        dependencies: Iterable["Dependant"] = (),
    ) -> "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]": ...

    @override
    def __call__(
        self,
        func: Union[
            Callable[P_HandlerParams, T_HandlerReturn],
            "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]",
            None,
        ] = None,
        *,
        filter: "Filter[StreamMessage[MsgType]]" = default_filter,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0"
            ),
        ] = (),
        dependencies: Iterable["Dependant"] = (),
    ) -> Union[
        "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]",
        Callable[
            [Callable[P_HandlerParams, T_HandlerReturn]],
            "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]",
        ],
    ]:
        if (options := self._call_options) is None:
            msg = (
                "You can't create subscriber directly. Please, use `add_call` at first."
            )
            raise SetupError(msg)

        total_deps = (*options.dependencies, *dependencies)
        total_middlewares = (*options.middlewares, *middlewares)
        async_filter: AsyncFilter[StreamMessage[MsgType]] = to_async(filter)

        def real_wrapper(
            func: Union[
                Callable[P_HandlerParams, T_HandlerReturn],
                "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]",
            ],
        ) -> "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]":
            handler = super(SubscriberUsecase, self).__call__(func)

            self.calls.append(
                HandlerItem[MsgType](
                    handler=handler,
                    filter=async_filter,
                    item_parser=parser or options.parser,
                    item_decoder=decoder or options.decoder,
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
            await self.close()

        except SystemExit:
            # Stop handler at `exit()` call
            await self.close()

            if app := self._state.get().di_state.context.get("app"):
                app.exit()

        except Exception:  # nosec B110
            # All other exceptions were logged by CriticalLogMiddleware
            pass

    async def process_message(self, msg: MsgType) -> "Response":
        """Execute all message processing stages."""
        broker_state = self._state.get()
        context: ContextRepo = broker_state.di_state.context
        logger_state = broker_state.logger_state

        async with AsyncExitStack() as stack:
            stack.enter_context(self.lock)

            # Enter context before middlewares
            stack.enter_context(context.scope("logger", logger_state.logger.logger))
            for k, v in self.extra_context.items():
                stack.enter_context(context.scope(k, v))

            # enter all middlewares
            middlewares: list[BaseMiddleware] = []
            for base_m in self.__build__middlewares_stack():
                middleware = base_m(msg, context=context)
                middlewares.append(middleware)
                await middleware.__aenter__()

            cache: dict[Any, Any] = {}
            parsing_error: Optional[Exception] = None
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
        logger_state = self._state.get().logger_state

        if self.ack_policy is AckPolicy.DO_NOTHING:
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
    ) -> Iterable["BasePublisherProto"]:
        if not message.reply_to or self._no_reply:
            return ()

        return self._make_response_publisher(message)

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
        log_level: int,
        message: str,
        extra: Optional["AnyDict"] = None,
        exc_info: Optional[Exception] = None,
    ) -> None:
        state = self._state.get()
        state.logger_state.logger.log(
            log_level,
            message,
            extra=extra,
            exc_info=exc_info,
        )
