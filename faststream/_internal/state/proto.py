from abc import abstractmethod
from typing import Protocol


class SetupAble(Protocol):
    @abstractmethod
    def _setup(self) -> None: ...
