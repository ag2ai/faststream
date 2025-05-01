from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    ContextManager,
    Dict,
    List,
    NoReturn,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

from typing_extensions import Literal, TypeAlias

from faststream.broker.middlewares.base import BaseMiddleware
from faststream.exceptions import IgnoredException
from faststream.utils import apply_types, context
from faststream.utils.functions import sync_fake_context, to_async

if TYPE_CHECKING:
    from types import TracebackType

    from faststream.broker.message import StreamMessage
    from faststream.types import AsyncFuncAny


GeneralExceptionHandler: TypeAlias = Union[
    Callable[..., None], Callable[..., Awaitable[None]]
]
PublishingExceptionHandler: TypeAlias = Callable[..., "Any"]

CastedGeneralExceptionHandler: TypeAlias = Callable[..., Awaitable[None]]
CastedPublishingExceptionHandler: TypeAlias = Callable[..., Awaitable["Any"]]
CastedHandlers: TypeAlias = List[
    Tuple[
        Type[Exception],
        CastedGeneralExceptionHandler,
    ]
]
CastedPublishingHandlers: TypeAlias = List[
    Tuple[
        Type[Exception],
        CastedPublishingExceptionHandler,
    ]
]


class BaseExceptionMiddleware(BaseMiddleware):
    def __init__(
        self,
        handlers: CastedHandlers,
        publish_handlers: CastedPublishingHandlers,
        msg: Optional[Any] = None,
    ) -> None:
        super().__init__(msg)
        self._handlers = handlers
        self._publish_handlers = publish_handlers

    async def consume_scope(
        self,
        call_next: "AsyncFuncAny",
        msg: "StreamMessage[Any]",
    ) -> Any:
        try:
            return await call_next(await self.on_consume(msg))

        except Exception as exc:
            exc_type = type(exc)

            for handler_type, handler in self._publish_handlers:
                if issubclass(exc_type, handler_type):
                    return await handler(exc)

            raise exc

    async def after_processed(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> Optional[bool]:
        if exc_type:
            for handler_type, handler in self._handlers:
                if issubclass(exc_type, handler_type):
                    # TODO: remove it after context will be moved to middleware
                    # In case parser/decoder error occurred
                    scope: ContextManager[Any]
                    if not context.get_local("message"):
                        scope = context.scope("message", self.msg)
                    else:
                        scope = sync_fake_context()

                    with scope:
                        await handler(exc_val)

                    return True

            return False

        return None


class ExceptionMiddleware:
    __slots__ = ("_handlers", "_publish_handlers")

    _handlers: CastedHandlers
    _publish_handlers: CastedPublishingHandlers

    def __init__(
        self,
        handlers: Optional[
            Dict[
                Type[Exception],
                GeneralExceptionHandler,
            ]
        ] = None,
        publish_handlers: Optional[
            Dict[
                Type[Exception],
                PublishingExceptionHandler,
            ]
        ] = None,
    ) -> None:
        self._handlers: CastedHandlers = [
            (IgnoredException, ignore_handler),
            *(
                (
                    exc_type,
                    apply_types(
                        cast("Callable[..., Awaitable[None]]", to_async(handler))
                    ),
                )
                for exc_type, handler in (handlers or {}).items()
            ),
        ]

        self._publish_handlers: CastedPublishingHandlers = [
            (IgnoredException, ignore_handler),
            *(
                (exc_type, apply_types(to_async(handler)))
                for exc_type, handler in (publish_handlers or {}).items()
            ),
        ]

    @overload
    def add_handler(
        self,
        exc: Type[Exception],
        publish: Literal[False] = False,
    ) -> Callable[[GeneralExceptionHandler], GeneralExceptionHandler]: ...

    @overload
    def add_handler(
        self,
        exc: Type[Exception],
        publish: Literal[True],
    ) -> Callable[[PublishingExceptionHandler], PublishingExceptionHandler]: ...

    def add_handler(
        self,
        exc: Type[Exception],
        publish: bool = False,
    ) -> Union[
        Callable[[GeneralExceptionHandler], GeneralExceptionHandler],
        Callable[[PublishingExceptionHandler], PublishingExceptionHandler],
    ]:
        if publish:

            def pub_wrapper(
                func: PublishingExceptionHandler,
            ) -> PublishingExceptionHandler:
                self._publish_handlers.append(
                    (
                        exc,
                        apply_types(to_async(func)),
                    )
                )
                return func

            return pub_wrapper

        else:

            def default_wrapper(
                func: GeneralExceptionHandler,
            ) -> GeneralExceptionHandler:
                self._handlers.append(
                    (
                        exc,
                        apply_types(to_async(func)),
                    )
                )
                return func

            return default_wrapper

    def __call__(self, msg: Optional[Any]) -> BaseExceptionMiddleware:
        """Real middleware runtime constructor."""
        return BaseExceptionMiddleware(
            handlers=self._handlers,
            publish_handlers=self._publish_handlers,
            msg=msg,
        )


async def ignore_handler(exception: IgnoredException) -> NoReturn:
    raise exception
