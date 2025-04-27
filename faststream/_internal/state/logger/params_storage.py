import logging
import warnings
from abc import abstractmethod
from typing import TYPE_CHECKING, Optional, Protocol
from weakref import WeakSet

from faststream._internal.constants import EMPTY

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict, LoggerProto
    from faststream._internal.context import ContextRepo


def make_logger_storage(
    logger: Optional["LoggerProto"],
    log_fmt: Optional[str],
    default_storage_cls: type["DefaultLoggerStorage"],
) -> "LoggerParamsStorage":
    if logger is EMPTY:
        return default_storage_cls(log_fmt)

    if log_fmt:
        warnings.warn(
            message="You can't set custom `logger` with `log_fmt` both.",
            category=RuntimeWarning,
            stacklevel=4,
        )

    return EmptyLoggerStorage() if logger is None else ManualLoggerStorage(logger)


class LoggerParamsStorage(Protocol):
    def setup_log_contest(self, params: "AnyDict") -> None: ...

    def get_logger(self, *, context: "ContextRepo") -> Optional["LoggerProto"]: ...

    def set_level(self, level: int) -> None: ...


class EmptyLoggerStorage(LoggerParamsStorage):
    def setup_log_contest(self, params: "AnyDict") -> None:
        pass

    def get_logger(self, *, context: "ContextRepo") -> None:
        return None

    def set_level(self, level: int) -> None:
        pass


class ManualLoggerStorage(LoggerParamsStorage):
    def __init__(self, logger: "LoggerProto") -> None:
        self.__logger = logger

    def setup_log_contest(self, params: "AnyDict") -> None:
        pass

    def get_logger(self, *, context: "ContextRepo") -> "LoggerProto":
        return self.__logger

    def set_level(self, level: int) -> None:
        if getattr(self.__logger, "setLevel", None):
            self.__logger.setLevel(level)  # type: ignore[attr-defined]


class DefaultLoggerStorage(LoggerParamsStorage):
    def __init__(self, log_fmt: Optional[str]) -> None:
        self._log_fmt = log_fmt

        self.logger_log_level = logging.INFO
        self._logger_ref = WeakSet[logging.Logger]()

    @abstractmethod
    def get_logger(self, *, context: "ContextRepo") -> "LoggerProto":
        raise NotImplementedError

    def _get_logger_ref(self) -> Optional[logging.Logger]:
        if self._logger_ref:
            return next(iter(self._logger_ref))
        return None

    def set_level(self, level: int) -> None:
        if lg := self._get_logger_ref():
            lg.setLevel(level)

        self.logger_log_level = level
