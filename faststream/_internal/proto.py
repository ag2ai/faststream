from abc import abstractmethod
from typing import Any, Callable, Optional, Protocol, TypeVar, Union, overload

from typing_extensions import Self

from faststream._internal.subscriber.call_wrapper import (
    HandlerCallWrapper,
    ensure_call_wrapper,
)
from faststream._internal.types import (
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)


class EndpointWrapper(Protocol[MsgType]):
    def __call__(
        self,
        func: Union[
            Callable[P_HandlerParams, T_HandlerReturn],
            HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn],
        ],
    ) -> HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]:
        handler: HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn] = (
            ensure_call_wrapper(func)
        )
        return handler


class Endpoint(EndpointWrapper[MsgType]):
    @abstractmethod
    def add_prefix(self, prefix: str) -> None: ...


NameRequiredCls = TypeVar("NameRequiredCls", bound="NameRequired")


class NameRequired:
    """Required name option object."""

    def __eq__(self, value: object, /) -> bool:
        """Compares the current object with another object for equality."""
        if value is None:
            return False

        if not isinstance(value, NameRequired):
            return NotImplemented

        return self.name == value.name

    def __init__(self, name: str) -> None:
        self.name = name

    @overload
    @classmethod
    def validate(
        cls: type[NameRequiredCls],
        value: Union[str, NameRequiredCls],
        **kwargs: Any,
    ) -> NameRequiredCls: ...

    @overload
    @classmethod
    def validate(
        cls: type[NameRequiredCls],
        value: None,
        **kwargs: Any,
    ) -> None: ...

    @classmethod
    def validate(
        cls,
        value: Union[str, Self, None],
        **kwargs: Any,
    ) -> Optional[Self]:
        """Factory to create object."""
        if value is not None and isinstance(value, str):
            value = cls(value, **kwargs)
        return value
