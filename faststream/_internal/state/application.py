from abc import ABC, abstractmethod

from faststream._internal.state.fast_depends import DIState


class ApplicationState(ABC):
    def __init__(self, di_state: DIState) -> None:
        self._di_state = di_state

    @property
    @abstractmethod
    def running(self) -> bool: ...

    @property
    def di_state(self) -> DIState:
        return self._di_state


class BasicApplicationState(ApplicationState):
    @property
    def running(self) -> bool:
        return False


class RunningApplicationState(ApplicationState):
    @property
    def running(self) -> bool:
        return True
