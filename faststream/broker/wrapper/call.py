import asyncio
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Generic,
    Iterable,
    List,
    Mapping,
    Optional,
    Reversible,
    Sequence,
    Union,
)
from unittest.mock import MagicMock

import anyio
from fast_depends.core import CallModel, build_call_model
from fast_depends.use import _InjectWrapper, inject

from faststream.broker.types import (
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.exceptions import SetupError
from faststream.utils.functions import to_async

if TYPE_CHECKING:
    from fast_depends.dependencies import Depends

    from faststream.broker.message import StreamMessage
    from faststream.broker.publisher.proto import PublisherProto
    from faststream.types import Decorator


class HandlerCallWrapper(Generic[MsgType, P_HandlerParams, T_HandlerReturn]):
    """A generic class to wrap handler calls."""

    mock: Optional[MagicMock]
    future: Optional["asyncio.Future[Any]"]
    is_test: bool

    _wrapped_call: Optional[Callable[..., Awaitable[Any]]]
    _original_call: Callable[P_HandlerParams, T_HandlerReturn]
    _publishers: List["PublisherProto[MsgType]"]

    __slots__ = (
        "_original_call",
        "_publishers",
        "_wrapped_call",
        "future",
        "is_test",
        "mock",
    )

    def __new__(
        cls,
        call: Union[
            "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]",
            Callable[P_HandlerParams, T_HandlerReturn],
        ],
    ) -> "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]":
        """Create a new instance of the class."""
        if isinstance(call, cls):
            return call
        else:
            return super().__new__(cls)

    def __init__(
        self,
        call: Callable[P_HandlerParams, T_HandlerReturn],
    ) -> None:
        """Initialize a handler."""
        if not isinstance(call, HandlerCallWrapper):
            self._original_call = call
            self._wrapped_call = None
            self._publishers = []

            self.mock = None
            self.future = None
            self.is_test = False

    def __call__(
        self,
        *args: P_HandlerParams.args,
        **kwargs: P_HandlerParams.kwargs,
    ) -> T_HandlerReturn:
        """Calls the object as a function."""
        return self._original_call(*args, **kwargs)

    def call_wrapped(
        self,
        message: "StreamMessage[MsgType]",
    ) -> Awaitable[Any]:
        """Calls the wrapped function with the given message."""
        assert self._wrapped_call, "You should use `set_wrapped` first"  # nosec B101
        if self.is_test:
            assert self.mock  # nosec B101
            self.mock(message._decoded_body)
        return self._wrapped_call(message)

    async def wait_call(self, timeout: Optional[float] = None) -> None:
        """Waits for a call with an optional timeout."""
        assert (  # nosec B101
            self.future is not None
        ), "You can use this method only with TestClient"
        with anyio.fail_after(timeout):
            await self.future

    def set_test(self) -> None:
        self.is_test = True
        if self.mock is None:
            self.mock = MagicMock()
        self.refresh(with_mock=True)

    def reset_test(self) -> None:
        self.is_test = False
        self.mock = None
        self.future = None

    def trigger(
        self,
        result: Any = None,
        error: Optional[BaseException] = None,
    ) -> None:
        if not self.is_test:
            return

        if self.future is None:
            raise SetupError("You can use this method only with TestClient")

        if self.future.done():
            self.future = asyncio.Future()

        if error:
            self.future.set_exception(error)
        else:
            self.future.set_result(result)

    def refresh(self, with_mock: bool = False) -> None:
        if asyncio.events._get_running_loop() is not None:
            self.future = asyncio.Future()

        if with_mock and self.mock is not None:
            self.mock.reset_mock()

    def set_wrapped(
        self,
        *,
        apply_types: bool,
        is_validate: bool,
        dependencies: Iterable["Depends"],
        _get_dependant: Optional[Callable[..., Any]],
        _call_decorators: Reversible["Decorator"],
    ) -> Optional["CallModel[..., Any]"]:
        call = self._original_call
        for decor in reversed(_call_decorators):
            call = decor(call)
        self._original_call = call

        f: Callable[..., Awaitable[Any]] = to_async(call)

        dependent: Optional[CallModel[..., Any]] = None
        if _get_dependant is None:
            dependent = build_call_model(
                f,
                cast=is_validate,
                extra_dependencies=dependencies,  # type: ignore[arg-type]
            )

            if apply_types:
                wrapper: _InjectWrapper[Any, Any] = inject(func=None)
                f = wrapper(func=f, model=dependent)

            f = _wrap_decode_message(
                func=f,
                params_ln=len(dependent.flat_params),
            )

        self._wrapped_call = f
        return dependent


def _wrap_decode_message(
    func: Callable[..., Awaitable[T_HandlerReturn]],
    params_ln: int,
) -> Callable[["StreamMessage[MsgType]"], Awaitable[T_HandlerReturn]]:
    """Wraps a function to decode a message and pass it as an argument to the wrapped function."""

    async def decode_wrapper(message: "StreamMessage[MsgType]") -> T_HandlerReturn:
        """A wrapper function to decode and handle a message."""
        msg = await message.decode()

        if params_ln > 1:
            if isinstance(msg, Mapping):
                return await func(**msg)
            elif isinstance(msg, Sequence):
                return await func(*msg)
        else:
            return await func(msg)

        raise AssertionError("unreachable")

    return decode_wrapper
