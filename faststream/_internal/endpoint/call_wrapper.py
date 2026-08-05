import asyncio
from collections.abc import Awaitable, Callable, Reversible, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
)
from unittest.mock import MagicMock

import anyio

from faststream._internal.configs import BrokerConfig
from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.parser import DefaultCodec
from faststream._internal.types import P_HandlerParams, T_HandlerReturn
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from fast_depends.core import CallModel
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import Decorator
    from faststream._internal.di import FastDependsConfig
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberUsecase
    from faststream.message import StreamMessage


def ensure_call_wrapper(
    call: Callable[P_HandlerParams, T_HandlerReturn],
    outer_config: BrokerConfig,
) -> "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]":
    if isinstance(call, HandlerCallWrapper):
        return call

    return HandlerCallWrapper(call, outer_config)


class HandlerCallWrapper(Generic[P_HandlerParams, T_HandlerReturn]):
    """A generic class to wrap handler calls."""

    future: Optional["asyncio.Future[Any]"]
    _wrapped_call: Callable[..., Awaitable[Any]] | None
    _original_call: Callable[P_HandlerParams, T_HandlerReturn]

    _publishers: list["PublisherProto[Any]"]

    # we have to store subscribers here
    # to protect them from garbage collection
    _subscribers: list["SubscriberUsecase[Any]"]

    __slots__ = (
        "_original_call",
        "_outer_config",
        "_publishers",
        "_subscribers",
        "_wrapped_call",
        "future",
        "is_test",
        "mock",
    )

    def __init__(
        self,
        call: Callable[P_HandlerParams, T_HandlerReturn],
        outer_config: BrokerConfig,
    ) -> None:
        """Initialize a handler."""
        self._original_call = call
        self._wrapped_call = None

        self._publishers = []
        self._subscribers = []

        self.mock = MagicMock()
        self.future = None
        self.is_test = False

        self._outer_config = outer_config

    def __call__(
        self,
        *args: P_HandlerParams.args,
        **kwargs: P_HandlerParams.kwargs,
    ) -> T_HandlerReturn:
        """Calls the object as a function."""
        return self._original_call(*args, **kwargs)

    def call_wrapped(
        self, context: ContextRepo
    ) -> Callable[["StreamMessage[Any]"], Awaitable[Any]]:
        async def _call_wrapped(message: "StreamMessage[Any]") -> Any:
            """Calls the wrapped function with the given message."""
            assert self._wrapped_call, "You should use `set_wrapped` first"
            if self.is_test:
                self.mock(message.body)
                self.mock.context = context.context

            return await self._wrapped_call(message)

        return _call_wrapped

    def set_wrapped(
        self,
        *,
        dependencies: Sequence["Dependant"],
        _call_decorators: Reversible["Decorator"],
        config: "FastDependsConfig",
    ) -> "CallModel":
        dependent = config.build_call(
            self._original_call,
            dependencies=dependencies,
            call_decorators=_call_decorators,
        )
        self._original_call = dependent.original_call
        self._wrapped_call = dependent.wrapped_call
        return dependent.dependent

    async def wait_call(self, timeout: float | None = None) -> None:
        """Waits for a call with an optional timeout."""
        assert self.future is not None, "You can use this method only with TestClient"
        with anyio.fail_after(timeout):
            await self.future

    def set_test(self) -> None:
        self.is_test = True
        self.mock.reset_mock()
        self.refresh(with_mock=True)

    def reset_test(self) -> None:
        self.is_test = False
        self.mock.reset_mock()
        self.future = None

    def trigger(
        self,
        result: Any = None,
        error: BaseException | None = None,
    ) -> None:
        if not self.is_test:
            return

        if self.future is None:
            msg = "You can use this method only with TestClient"
            raise SetupError(msg)

        if self.future.done():
            self.future = asyncio.Future()

        if error:
            self.future.set_exception(error)
            # Mark the mirrored error as retrieved to avoid unhandled-future reports.
            self.future.exception()

        else:
            self.future.set_result(result)

    def refresh(self, with_mock: bool = False) -> None:
        if asyncio.events._get_running_loop() is not None:
            self.future = asyncio.Future()

        if with_mock and self.mock is not None:
            self.mock.reset_mock()

    async def assert_called_once_with(
        self,
        body: Any = EMPTY,
        context: dict[str, Any] = EMPTY,
    ) -> None:
        if not self.is_test:
            return

        if body != EMPTY:
            serializer = self._outer_config.fd_config._serializer
            codec = self._outer_config.broker_codec or DefaultCodec()

            encoded_message, _ = await codec.encode(body, serializer)
            self.mock.assert_called_once_with(encoded_message)

        if context != EMPTY:
            context_repo = ContextRepo(self.mock.context)
            for key, value in context.items():
                assert context_repo.resolve(key) == value
