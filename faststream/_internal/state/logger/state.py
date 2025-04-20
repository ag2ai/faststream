from typing import TYPE_CHECKING, Optional

from faststream._internal.state.proto import SetupAble

from .logger_proxy import (
    EmptyLoggerObject,
    LoggerObject,
    NotSetLoggerObject,
    RealLoggerObject,
)
from .params_storage import (
    DefaultLoggerStorage,
    LoggerParamsStorage,
    make_logger_storage,
)

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict, LoggerProto
    from faststream._internal.context import ContextRepo


def make_logger_state(
    logger: Optional["LoggerProto"],
    log_level: int,
    log_fmt: Optional[str],
    default_storage_cls: type["DefaultLoggerStorage"],
) -> "LoggerState":
    storage = make_logger_storage(
        logger=logger,
        log_fmt=log_fmt,
        default_storage_cls=default_storage_cls,
    )

    return LoggerState(
        log_level=log_level,
        storage=storage,
    )


class LoggerState(SetupAble):
    def __init__(
        self,
        log_level: int,
        storage: LoggerParamsStorage,
    ) -> None:
        self.log_level = log_level
        self.params_storage = storage

        self.logger: LoggerObject = NotSetLoggerObject()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(log_level={self.log_level}, logger={self.logger})"

    def set_level(self, level: int) -> None:
        self.log_level = level
        self.params_storage.set_level(level)

    def log(
        self,
        message: str,
        log_level: Optional[int] = None,
        extra: Optional["AnyDict"] = None,
        exc_info: Optional[BaseException] = None,
    ) -> None:
        self.logger.log(
            (log_level or self.log_level),
            message,
            extra=extra,
            exc_info=exc_info,
        )

    def _setup(  # type: ignore[override]
        self,
        *,
        context: "ContextRepo",
    ) -> None:
        if not self.logger:
            if logger := self.params_storage.get_logger(context=context):
                self.logger = RealLoggerObject(logger)
            else:
                self.logger = EmptyLoggerObject()
