from datetime import datetime
from decimal import Decimal
from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
)

from typing_extensions import ParamSpec, TypeAlias

AnyDict: TypeAlias = Dict[str, Any]
AnyHttpUrl: TypeAlias = str

F_Return = TypeVar("F_Return")
F_Spec = ParamSpec("F_Spec")

AnyCallable: TypeAlias = Callable[..., Any]
NoneCallable: TypeAlias = Callable[..., None]
AsyncFunc: TypeAlias = Callable[..., Awaitable[Any]]

DecoratedCallable: TypeAlias = AnyCallable
DecoratedCallableNone: TypeAlias = NoneCallable

JsonArray: TypeAlias = Sequence["DecodedMessage"]

JsonTable: TypeAlias = Dict[str, "DecodedMessage"]

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

SendableTable: TypeAlias = Dict[str, "BaseSendableMessage"]


class StandardDataclass(Protocol):
    """Protocol to check type is dataclass."""

    __dataclass_fields__: ClassVar[Dict[str, Any]]
    __dataclass_params__: ClassVar[Any]
    __post_init__: ClassVar[Callable[..., None]]

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Interface method."""
        ...


BaseSendableMessage: TypeAlias = Union[
    JsonDecodable,
    Decimal,
    datetime,
    None,
    StandardDataclass,
    SendableTable,
    SendableArray,
]

try:
    from faststream._compat import BaseModel

    SendableMessage: TypeAlias = Union[
        BaseModel,
        BaseSendableMessage,
    ]

except ImportError:
    SendableMessage: TypeAlias = BaseSendableMessage  # type: ignore[no-redef]

SettingField: TypeAlias = Union[
    bool,
    str,
    List[Union[bool, str]],
    List[str],
    List[bool],
]

Lifespan: TypeAlias = Callable[..., AsyncContextManager[None]]


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
