from abc import abstractmethod
from contextlib import AsyncExitStack
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

from typing_extensions import Self, override

from faststream.asyncapi.abc import AsyncAPIOperation
from faststream.asyncapi.message import parse_handler_params
from faststream.asyncapi.utils import to_camelcase
from faststream.broker.response import ensure_response
from faststream.broker.subscriber.call_item import HandlerItem
from faststream.broker.subscriber.proto import SubscriberProto
from faststream.broker.types import (
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.broker.utils import MultiLock, get_watcher_context, resolve_custom_func
from faststream.broker.wrapper.call import HandlerCallWrapper
from faststream.exceptions import SetupError, StopConsume, SubscriberNotFound
from faststream.utils.context.repository import context
from faststream.utils.functions import sync_fake_context, to_async

if TYPE_CHECKING:
    from fast_depends.dependencies import Depends

    from faststream.broker.message import StreamMessage
    from faststream.broker.middlewares import BaseMiddleware
    from faststream.broker.publisher.proto import BasePublisherProto, ProducerProto
    from faststream.broker.response import Response
    from faststream.broker.types import (
        AsyncCallable,
        BrokerMiddleware,
        CustomCallable,
        Filter,
        SubscriberMiddleware,
    )
    from faststream.types import AnyDict, Decorator, LoggerProto


class _CallOptions:
    __slots__ = (
        "decoder",
        "dependencies",
        "filter",
        "middlewares",
        "parser",
    )

    def __init__(
        self,
        *,
        filter: "Filter[Any]",
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
        middlewares: Sequence["SubscriberMiddleware[Any]"],
        dependencies: Iterable["Depends"],
    ) -> None:
        self.filter = filter
        self.parser = parser
        self.decoder = decoder
        self.middlewares = middlewares
        self.dependencies = dependencies


class SubscriberUsecase(
    AsyncAPIOperation,
    SubscriberProto[MsgType],
):
    """A class representing an asynchronous handler."""

    lock: ContextManager[Any]
    extra_watcher_options: "AnyDict"
    extra_context: "AnyDict"
    graceful_timeout: Optional[float]
    logger: Optional["LoggerProto"]

    _broker_dependencies: Iterable["Depends"]
    _call_options: Optional["_CallOptions"]
    _call_decorators: Iterable["Decorator"]

    def __init__(
        self,
        *,
        no_ack: bool,
        no_reply: bool,
        retry: Union[bool, int],
        broker_dependencies: Iterable["Depends"],
        broker_middlewares: Sequence["BrokerMiddleware[MsgType]"],
        default_parser: "AsyncCallable",
        default_decoder: "AsyncCallable",
        # AsyncAPI information
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        """Initialize a new instance of the class."""
        self.calls = []

        self._parser = default_parser
        self._decoder = default_decoder
        self._no_reply = no_reply
        # Watcher args
        self._no_ack = no_ack
        self._retry = retry

        self._call_options = None
        self._call_decorators = ()
        self.running = False
        self.lock = sync_fake_context()

        # Setup in include
        self._broker_dependencies = broker_dependencies
        self._broker_middlewares = broker_middlewares

        # register in setup later
        self._producer = None
        self.graceful_timeout = None
        self.extra_context = {}
        self.extra_watcher_options = {}

        # AsyncAPI
        self.title_ = title_
        self.description_ = description_
        self.include_in_schema = include_in_schema

    def add_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None:
        self._broker_middlewares = (*self._broker_middlewares, middleware)

    @override
    def setup(  # type: ignore[override]
        self,
        *,
        logger: Optional["LoggerProto"],
        producer: Optional["ProducerProto"],
        graceful_timeout: Optional[float],
        extra_context: "AnyDict",
        # broker options
        broker_parser: Optional["CustomCallable"],
        broker_decoder: Optional["CustomCallable"],
        # dependant args
        apply_types: bool,
        is_validate: bool,
        _get_dependant: Optional[Callable[..., Any]],
        _call_decorators: Iterable["Decorator"],
    ) -> None:
        self.lock = MultiLock()

        self._producer = producer
        self.graceful_timeout = graceful_timeout
        self.extra_context = extra_context
        self.logger = logger

        self.watcher = get_watcher_context(logger, self._no_ack, self._retry)

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

            call.setup(
                parser=async_parser,
                decoder=async_decoder,
                apply_types=apply_types,
                is_validate=is_validate,
                _get_dependant=_get_dependant,
                _call_decorators=(*self._call_decorators, *_call_decorators),
                broker_dependencies=self._broker_dependencies,
            )

            call.handler.refresh(with_mock=False)

    @abstractmethod
    async def start(self) -> None:
        """Start the handler."""
        self.running = True

    @abstractmethod
    async def stop(self) -> None:
        """Close the handler.

        Blocks event loop up to graceful_timeout seconds.
        """
        self.running = False
        if isinstance(self.lock, MultiLock):
            await self.lock.wait_release(self.graceful_timeout)

    def add_call(
        self,
        *,
        filter_: "Filter[Any]",
        parser_: Optional["CustomCallable"],
        decoder_: Optional["CustomCallable"],
        middlewares_: Sequence["SubscriberMiddleware[Any]"],
        dependencies_: Iterable["Depends"],
    ) -> Self:
        self._call_options = _CallOptions(
            filter=filter_,
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
        filter: Optional["Filter[Any]"] = None,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        dependencies: Iterable["Depends"] = (),
    ) -> Callable[
        [Callable[P_HandlerParams, T_HandlerReturn]],
        "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]",
    ]: ...

    @overload
    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn],
        *,
        filter: Optional["Filter[Any]"] = None,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        dependencies: Iterable["Depends"] = (),
    ) -> "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]": ...

    def __call__(
        self,
        func: Optional[Callable[P_HandlerParams, T_HandlerReturn]] = None,
        *,
        filter: Optional["Filter[Any]"] = None,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        dependencies: Iterable["Depends"] = (),
    ) -> Any:
        if (options := self._call_options) is None:
            raise SetupError(
                "You can't create subscriber directly. Please, use `add_call` at first."
            )

        total_deps = (*options.dependencies, *dependencies)
        total_middlewares = (*options.middlewares, *middlewares)

        def real_wrapper(
            func: Callable[P_HandlerParams, T_HandlerReturn],
        ) -> "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]":
            handler = HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn](
                func
            )

            self.calls.append(
                HandlerItem[MsgType](
                    handler=handler,
                    filter=to_async(filter or options.filter),
                    item_parser=parser or options.parser,
                    item_decoder=decoder or options.decoder,
                    item_middlewares=total_middlewares,
                    dependencies=total_deps,
                )
            )

            return handler

        if func is None:
            return real_wrapper

        else:
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
            try:
                await self.stop()
            finally:
                # Ensure that app.exit() is called on a shutdown request
                # even if the consumer close operation threw an error.
                if app := context.get("app"):
                    app.exit()

        except Exception:  # nosec B110
            # All other exceptions were logged by CriticalLogMiddleware
            pass

    async def process_message(self, msg: MsgType) -> "Response":
        """Execute all message processing stages."""
        async with AsyncExitStack() as stack:
            stack.enter_context(self.lock)

            # Enter context before middlewares
            for k, v in self.extra_context.items():
                stack.enter_context(context.scope(k, v))

            stack.enter_context(context.scope("handler_", self))

            # enter all middlewares
            middlewares: List[BaseMiddleware] = []
            for base_m in self._broker_middlewares:
                middleware = base_m(msg)
                middlewares.append(middleware)
                await middleware.__aenter__()

            cache: Dict[Any, Any] = {}
            parsing_error: Optional[Exception] = None
            for h in self.calls:
                try:
                    message = await h.is_suitable(msg, cache)
                except Exception as e:
                    parsing_error = e
                    break

                if message is not None:
                    # Acknowledgement scope
                    # TODO: move it to scope enter at `retry` option deprecation
                    await stack.enter_async_context(
                        self.watcher(
                            message,
                            **self.extra_watcher_options,
                        )
                    )

                    stack.enter_context(
                        context.scope("log_context", self.get_log_context(message))
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
                        )
                    )

                    if not result_msg.correlation_id:
                        result_msg.correlation_id = message.correlation_id

                    for p in chain(
                        self.__get_response_publisher(message),
                        h.handler._publishers,
                    ):
                        await p.publish(
                            result_msg.body,
                            **result_msg.as_publish_kwargs(),
                            # publisher middlewares
                            _extra_middlewares=[
                                m.publish_scope for m in middlewares[::-1]
                            ],
                        )

                    # Return data for tests
                    return result_msg

            # Suitable handler was not found or
            # parsing/decoding exception occurred
            for m in middlewares:
                stack.push_async_exit(m.__aexit__)

            if parsing_error:
                raise parsing_error

            else:
                raise SubscriberNotFound(f"There is no suitable handler for {msg=}")

        # An error was raised and processed by some middleware
        return ensure_response(None)

    def __get_response_publisher(
        self,
        message: "StreamMessage[MsgType]",
    ) -> Iterable["BasePublisherProto"]:
        if not message.reply_to or self._no_reply:
            return ()

        else:
            return self._make_response_publisher(message)

    def get_log_context(
        self,
        message: Optional["StreamMessage[MsgType]"],
    ) -> Dict[str, str]:
        """Generate log context."""
        return {
            "message_id": getattr(message, "message_id", ""),
        }

    # AsyncAPI methods

    @property
    def call_name(self) -> str:
        """Returns the name of the handler call."""
        if not self.calls:
            return "Subscriber"

        if len(self.calls) == 1:
            return to_camelcase(self.calls[0].call_name)

        return f"[{','.join(to_camelcase(c.call_name) for c in self.calls)}]"

    def get_description(self) -> Optional[str]:
        """Returns the description of the handler."""
        if not self.calls:  # pragma: no cover
            return None

        if len(self.calls) == 1:
            return self.calls[0].description

        return "\n".join(
            f"{to_camelcase(h.call_name)}: {h.description}" for h in self.calls
        )

    def get_payloads(self) -> List[Tuple["AnyDict", str]]:
        """Get the payloads of the handler."""
        payloads: List[Tuple[AnyDict, str]] = []

        for h in self.calls:
            if h.dependant is None:
                raise SetupError("You should setup `Handler` at first.")

            body = parse_handler_params(
                h.dependant,
                prefix=f"{self.title_ or self.call_name}:Message",
            )

            payloads.append((body, to_camelcase(h.call_name)))

        if not self.calls:
            payloads.append(
                (
                    {
                        "title": f"{self.title_ or self.call_name}:Message:Payload",
                    },
                    to_camelcase(self.call_name),
                )
            )

        return payloads

    def _log(
        self,
        log_level: int,
        message: str,
        extra: Optional["AnyDict"] = None,
        exc_info: Optional[Exception] = None,
    ) -> None:
        if self.logger is not None:
            self.logger.log(
                log_level,
                message,
                extra=extra,
                exc_info=exc_info,
            )
