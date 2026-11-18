from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Specification(Protocol):
    __slots__ = ()

    @property
    def title(self) -> str: ...

    def to_json(self) -> str: ...

    def to_jsonable(self) -> Any: ...

    def to_yaml(self) -> str: ...
