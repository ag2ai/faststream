from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from decimal import Decimal
from typing import (
    Any,
    ClassVar,
    Protocol,
    TypeAlias,
    TypeVar,
)

from typing_extensions import ParamSpec

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

JsonDecodable: TypeAlias = bool | bytes | bytearray | float | int | str | None

DecodedMessage: TypeAlias = JsonDecodable | JsonArray | JsonTable

SendableArray: TypeAlias = Sequence["BaseSendableMessage"]

SendableTable: TypeAlias = dict[str, "BaseSendableMessage"]


class StandardDataclass(Protocol):
    """Protocol to check type is dataclass."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


BaseSendableMessage: TypeAlias = (
    JsonDecodable
    | Decimal
    | datetime
    | StandardDataclass
    | SendableTable
    | SendableArray
    | None
)

try:
    from pydantic import BaseModel

    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False

try:
    from msgspec import Struct

    HAS_MSGSPEC = True
except ImportError:
    HAS_MSGSPEC = False

if HAS_PYDANTIC and HAS_MSGSPEC:
    SendableMessage: TypeAlias = Struct | BaseModel | BaseSendableMessage
elif HAS_PYDANTIC:
    SendableMessage: TypeAlias = BaseModel | BaseSendableMessage  # type: ignore[no-redef,misc]
elif HAS_MSGSPEC:
    SendableMessage: TypeAlias = Struct | BaseSendableMessage  # type: ignore[no-redef,misc]
else:
    SendableMessage: TypeAlias = BaseSendableMessage  # type: ignore[no-redef,misc]

SettingField: TypeAlias = (
    bool | str | list[bool | str] | list[str] | list[bool] | int | None
)

Lifespan: TypeAlias = Callable[..., AbstractAsyncContextManager[None]]


class LoggerProto(Protocol):
    def log(
        self,
        level: int,
        msg: Any,
        /,
        *,
        exc_info: Any = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None: ...
