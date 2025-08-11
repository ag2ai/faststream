import logging
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Protocol
from weakref import WeakSet

from faststream._internal.constants import EMPTY

if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.context import ContextRepo


def make_logger_storage(
    logger: Optional["LoggerProto"],
    default_storage_cls: type["DefaultLoggerStorage"],
) -> "LoggerParamsStorage":
    if logger is EMPTY:
        return default_storage_cls()

    return EmptyLoggerStorage() if logger is None else ManualLoggerStorage(logger)


class LoggerParamsStorage(Protocol):
    def register_subscriber(self, params: dict[str, Any]) -> None: ...

    def get_logger(self, *, context: "ContextRepo") -> Optional["LoggerProto"]: ...

    def set_level(self, level: int) -> None: ...


class EmptyLoggerStorage(LoggerParamsStorage):
    def register_subscriber(self, params: dict[str, Any]) -> None:
        pass

    def get_logger(self, *, context: "ContextRepo") -> None:
        return None

    def set_level(self, level: int) -> None:
        pass


class ManualLoggerStorage(LoggerParamsStorage):
    def __init__(self, logger: "LoggerProto") -> None:
        self.__logger = logger

    def register_subscriber(self, params: dict[str, Any]) -> None:
        pass

    def get_logger(self, *, context: "ContextRepo") -> "LoggerProto":
        return self.__logger

    def set_level(self, level: int) -> None:
        if getattr(self.__logger, "setLevel", None):
            self.__logger.setLevel(level)  # type: ignore[attr-defined]


class DefaultLoggerStorage(LoggerParamsStorage):
    def __init__(self) -> None:
        # will be used to build logger in `get_logger` method
        self.logger_log_level = logging.INFO

        self._logger_ref = WeakSet[logging.Logger]()

    @abstractmethod
    def get_logger(self, *, context: "ContextRepo") -> "LoggerProto":
        raise NotImplementedError

    def _get_logger_ref(self) -> logging.Logger | None:
        if self._logger_ref:
            return next(iter(self._logger_ref))

        return None

    def set_level(self, level: int) -> None:
        if lg := self._get_logger_ref():
            lg.setLevel(level)

        self.logger_log_level = level
