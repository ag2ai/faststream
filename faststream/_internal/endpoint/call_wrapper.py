import asyncio
from collections.abc import Awaitable, Callable, Reversible, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
)

import anyio

from faststream._internal.configs import BrokerConfig
from faststream._internal.constants import EMPTY
from faststream._internal.testing.calls import CallRecorder
from faststream._internal.types import P_HandlerParams, T_HandlerReturn
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from collections.abc import Mapping
    from unittest.mock import MagicMock

    from fast_depends.core import CallModel
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import Decorator
    from faststream._internal.context import ContextRepo
    from faststream._internal.di import FastDependsConfig
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberUsecase
    from faststream._internal.types import AsyncCallable
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
    # The handler as it was written, kept as written so that composing it again
    # from an unchanged declaration produces an unchanged result.
    _declared_call: Callable[P_HandlerParams, T_HandlerReturn]
    # What the last composition made of it, decorators applied.
    _composed_call: Callable[P_HandlerParams, T_HandlerReturn]

    _publishers: list["PublisherProto[Any]"]

    # we have to store subscribers here
    # to protect them from garbage collection
    _subscribers: list["SubscriberUsecase[Any]"]

    __slots__ = (
        "_composed_call",
        "_declared_call",
        "_publishers",
        "_recorder",
        "_subscribers",
        "_wrapped_call",
        "future",
        "is_test",
    )

    def __init__(
        self,
        call: Callable[P_HandlerParams, T_HandlerReturn],
        outer_config: BrokerConfig,
    ) -> None:
        """Initialize a handler."""
        self._declared_call = call
        self._composed_call = call
        self._wrapped_call = None

        self._publishers = []
        self._subscribers = []

        self._recorder = CallRecorder(
            getattr(call, "__name__", repr(call)),
            outer_config,
        )
        self.future = None
        self.is_test = False

    def __call__(
        self,
        *args: P_HandlerParams.args,
        **kwargs: P_HandlerParams.kwargs,
    ) -> T_HandlerReturn:
        """Calls the object as a function."""
        return self._composed_call(*args, **kwargs)

    @property
    def _original_call(self) -> Callable[P_HandlerParams, T_HandlerReturn]:
        """The composed call, under the name it had before the two were kept apart."""
        return self._composed_call

    @property
    def mock(self) -> "MagicMock":
        """The mock recording the handler's calls, available under a test broker."""
        if not self.is_test:
            msg = (
                f"`{self._recorder.name}` is not under a test broker: "
                "wrap the broker with its `Test*Broker` to access the mock."
            )
            raise SetupError(msg)
        return self._recorder.mock

    async def assert_called_once_with(
        self,
        body: Any = EMPTY,
        /,
        *,
        headers: Any = EMPTY,
        correlation_id: Any = EMPTY,
        reply_to: Any = EMPTY,
        content_type: Any = EMPTY,
        path: Any = EMPTY,
        context: "Mapping[str, Any]" = EMPTY,
    ) -> None:
        """Assert the handler was called once, with the message described here.

        Headers match as a subset; every other field matches exactly.
        """
        self.mock.assert_called_once()
        await self._recorder.assert_called_once_with(
            body,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            content_type=content_type,
            path=path,
            context=context,
        )

    def call_wrapped(
        self,
        context: "ContextRepo",
        decoder: "AsyncCallable",
    ) -> Callable[["StreamMessage[Any]"], Awaitable[Any]]:
        async def _call_wrapped(message: "StreamMessage[Any]") -> Any:
            """Calls the wrapped function with the given message."""
            assert self._wrapped_call, "You should use `set_wrapped` first"
            if self.is_test:
                await self._recorder.record(message, context=context, decoder=decoder)

            return await self._wrapped_call(message)

        return _call_wrapped

    def set_wrapped(
        self,
        *,
        dependencies: Sequence["Dependant"],
        _call_decorators: Reversible["Decorator"],
        config: "FastDependsConfig",
    ) -> "CallModel":
        # Composed from the declaration rather than from the last composition:
        # `build_call` answers with the call it decorated, so reading that back
        # as the input would decorate it again, one more layer per build.
        dependent = config.build_call(
            self._declared_call,
            dependencies=dependencies,
            call_decorators=_call_decorators,
        )
        self._composed_call = dependent.original_call
        self._wrapped_call = dependent.wrapped_call
        return dependent.dependent

    async def wait_call(self, timeout: float | None = None) -> None:
        """Waits for a call with an optional timeout."""
        assert self.future is not None, "You can use this method only with TestClient"
        with anyio.fail_after(timeout):
            await self.future

    def set_test(self) -> None:
        self.is_test = True
        self.refresh(with_mock=True)

    def reset_test(self) -> None:
        self.is_test = False
        self._recorder.reset()
        self._recorder.stop_mirroring()
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

        if with_mock:
            self._recorder.reset()
