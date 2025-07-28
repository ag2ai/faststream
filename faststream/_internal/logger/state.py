import logging
from typing import TYPE_CHECKING, Any, Optional

from .logger_proxy import (
    EmptyLoggerObject,
    LoggerObject,
    NotSetLoggerObject,
    RealLoggerObject,
)
from .params_storage import (
    DefaultLoggerStorage,
    EmptyLoggerStorage,
    LoggerParamsStorage,
    make_logger_storage,
)

if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.context import ContextRepo


def make_logger_state(
    logger: Optional["LoggerProto"],
    log_level: int,
    default_storage_cls: type["DefaultLoggerStorage"],
) -> "LoggerState":
    storage = make_logger_storage(
        logger=logger,
        default_storage_cls=default_storage_cls,
    )

    return LoggerState(
        log_level=log_level,
        storage=storage,
    )


class LoggerState:
    def __init__(
        self,
        log_level: int = logging.INFO,
        storage: Optional["LoggerParamsStorage"] = None,
    ) -> None:
        self.log_level = log_level
        self.params_storage = storage or EmptyLoggerStorage()

        self.logger: LoggerObject = NotSetLoggerObject()

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(log_level={self.log_level}, logger={self.logger})"
        )

    def set_level(self, level: int) -> None:
        self.log_level = level
        self.params_storage.set_level(level)

    def log(
        self,
        message: str,
        log_level: int | None = None,
        extra: dict[str, Any] | None = None,
        exc_info: BaseException | None = None,
    ) -> None:
        self.logger.log(
            (log_level or self.log_level),
            message,
            extra=extra,
            exc_info=exc_info,
        )

    def _setup(self, context: "ContextRepo", /) -> None:
        if not self.logger:
            if logger := self.params_storage.get_logger(context=context):
                self.logger = RealLoggerObject(logger)
            else:
                self.logger = EmptyLoggerObject()
