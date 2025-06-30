from abc import abstractmethod
from typing import Any, Protocol, runtime_checkable

from .schema import BaseApplicationSchema


@runtime_checkable
class Specification(Protocol):
    @property
    @abstractmethod
    def schema(self) -> BaseApplicationSchema: ...

    def to_json(self) -> str:
        return self.schema.to_json()

    def to_jsonable(self) -> Any:
        return self.schema.to_jsonable()

    def to_yaml(self) -> str:
        return self.schema.to_yaml()
