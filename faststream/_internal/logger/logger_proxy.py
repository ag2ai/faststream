from abc import abstractmethod
from collections.abc import Mapping
from typing import Any, Optional

from faststream._internal.basic_types import LoggerProto
from faststream.exceptions import IncorrectState


class LoggerObject(LoggerProto):
    logger: Optional["LoggerProto"]

    @abstractmethod
    def __bool__(self) -> bool: ...


class NotSetLoggerObject(LoggerObject):
    """Default logger proxy for state.

    Raises an error if user tries to log smth before state setup.
    """

    def __init__(self) -> None:
        self.logger = None

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def log(
        self,
        level: int,
        msg: Any,
        /,
        *,
        exc_info: Any = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        err_msg = (
            "Logger object not set. Please, call `broker.connect()` or "
            "`_setup_logger_state` of parent broker state."
        )
        raise IncorrectState(err_msg)


class EmptyLoggerObject(LoggerObject):
    """Empty logger proxy for state.

    Will be used if user setup `logger=None`.
    """

    def __init__(self) -> None:
        self.logger = None

    def __bool__(self) -> bool:
        return True

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def log(
        self,
        level: int,
        msg: Any,
        /,
        *,
        exc_info: Any = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        pass


class RealLoggerObject(LoggerObject):
    """Empty logger proxy for state.

    Will be used if user setup custom `logger` (.params_storage.ManualLoggerStorage)
    or in default logger case (.params_storage.DefaultLoggerStorage).
    """

    logger: "LoggerProto"

    def __init__(self, logger: "LoggerProto") -> None:
        self.logger = logger

    def __bool__(self) -> bool:
        return True

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(logger={self.logger})"

    def log(
        self,
        level: int,
        msg: Any,
        /,
        *,
        exc_info: Any = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        self.logger.log(
            level,
            msg,
            extra=extra,
            exc_info=exc_info,
        )
