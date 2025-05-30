import logging
import warnings
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional

from typing_extensions import Annotated, Doc, deprecated

from faststream.broker.core.abc import ABCBroker
from faststream.broker.types import MsgType
from faststream.types import EMPTY

if TYPE_CHECKING:
    from faststream.types import AnyDict, LoggerProto


class LoggingBroker(ABCBroker[MsgType]):
    """A mixin class for logging."""

    logger: Optional["LoggerProto"]

    @abstractmethod
    def get_fmt(self) -> str:
        """Fallback method to get log format if `log_fmt` if not specified."""
        raise NotImplementedError()

    @abstractmethod
    def _setup_log_context(self) -> None:
        raise NotImplementedError()

    def __init__(
        self,
        *args: Any,
        default_logger: Annotated[
            logging.Logger,
            Doc("Logger object to use if `logger` is not set."),
        ],
        logger: Annotated[
            Optional["LoggerProto"],
            Doc("User specified logger to pass into Context and log service messages."),
        ],
        log_level: Annotated[
            int,
            Doc("Service messages log level."),
        ],
        log_fmt: Annotated[
            Optional[str],
            deprecated(
                "Argument `log_fmt` is deprecated since 0.5.42 and will be removed in 0.6.0. "
                "Pass a pre-configured `logger` instead."
            ),
            Doc("Default logger log format."),
        ] = EMPTY,
        **kwargs: Any,
    ) -> None:
        if logger is not EMPTY:
            self.logger = logger
            self.use_custom = True
        else:
            self.logger = default_logger
            self.use_custom = False

        self._msg_log_level = log_level

        if log_fmt is not EMPTY:
            warnings.warn(
                DeprecationWarning(
                    "Argument `log_fmt` is deprecated since 0.5.42 and will be removed in 0.6.0. "
                    "Pass a pre-configured `logger` instead."
                ),
                stacklevel=2,
            )
            self._fmt = log_fmt
        else:
            self._fmt = None

        super().__init__(*args, **kwargs)

    def _get_fmt(self) -> str:
        """Get default logger format at broker startup."""
        return self._fmt or self.get_fmt()

    def _log(
        self,
        message: Annotated[
            str,
            Doc("Log message."),
        ],
        log_level: Annotated[
            Optional[int],
            Doc("Log record level. Use `__init__: log_level` option if not specified."),
        ] = None,
        extra: Annotated[
            Optional["AnyDict"],
            Doc("Log record extra information."),
        ] = None,
        exc_info: Annotated[
            Optional[Exception],
            Doc("Exception object to log traceback."),
        ] = None,
    ) -> None:
        """Logs a message."""
        if self.logger is not None:
            self.logger.log(
                (log_level or self._msg_log_level),
                message,
                extra=extra,
                exc_info=exc_info,
            )
