from collections.abc import Awaitable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from decimal import Decimal
from typing import (
    Any,
    Callable,
    ClassVar,
    Optional,
    Protocol,
    TypeVar,
    Union,
)

from typing_extensions import ParamSpec, TypeAlias

AnyDict: TypeAlias = dict[str, Any]
AnyHttpUrl: TypeAlias = str

F_Return = TypeVar("F_Return")
F_Spec = ParamSpec("F_Spec")

AnyCallable: TypeAlias = Callable[..., Any]
NoneCallable: TypeAlias = Callable[..., None]
AsyncFunc: TypeAlias = Callable[..., Awaitable[Any]]
AsyncFuncAny: TypeAlias = Callable[[Any], Awaitable[Any]]

DecoratedCallable: TypeAlias = AnyCallable
DecoratedCallableNone: TypeAlias = NoneCallable

Decorator: TypeAlias = Callable[[AnyCallable], AnyCallable]

JsonArray: TypeAlias = Sequence["DecodedMessage"]

JsonTable: TypeAlias = dict[str, "DecodedMessage"]

JsonDecodable: TypeAlias = Union[
    bool,
    bytes,
    bytearray,
    float,
    int,
    str,
    None,
]

DecodedMessage: TypeAlias = Union[
    JsonDecodable,
    JsonArray,
    JsonTable,
]

SendableArray: TypeAlias = Sequence["BaseSendableMessage"]

SendableTable: TypeAlias = dict[str, "BaseSendableMessage"]


class StandardDataclass(Protocol):
    """Protocol to check type is dataclass."""

    __dataclass_fields__: ClassVar[AnyDict]


BaseSendableMessage: TypeAlias = Union[
    JsonDecodable,
    Decimal,
    datetime,
    StandardDataclass,
    SendableTable,
    SendableArray,
    None,
]

try:
    from faststream._internal._compat import BaseModel

    SendableMessage: TypeAlias = Union[
        BaseModel,
        BaseSendableMessage,
    ]

except ImportError:
    SendableMessage: TypeAlias = BaseSendableMessage  # type: ignore[no-redef,misc]

SettingField: TypeAlias = Union[
    bool,
    str,
    list[Union[bool, str]],
    list[str],
    list[bool],
]

Lifespan: TypeAlias = Callable[..., AbstractAsyncContextManager[None]]


class LoggerProto(Protocol):
    def log(
        self,
        level: int,
        msg: Any,
        /,
        *,
        exc_info: Any = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None: ...
