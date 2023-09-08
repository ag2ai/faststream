import logging
from typing import Any, Optional

from faststream.broker.message import StreamMessage
from faststream.broker.types import MsgType
from faststream.log import access_logger
from faststream.types import AnyDict


class LoggingMixin:
    """A mixin class for logging.

    Attributes:
        logger : logger object used for logging
        log_level : log level for logging
        _fmt : format string for log messages

    Methods:
        fmt : getter method for _fmt attribute
        _get_log_context : returns a dictionary with log context information
        _log : logs a message with optional log level, extra data, and exception info
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    def __init__(
        self,
        *args: Any,
        logger: Optional[logging.Logger] = access_logger,
        log_level: int = logging.INFO,
        log_fmt: Optional[str] = "%(asctime)s %(levelname)s - %(message)s",
        **kwargs: Any,
    ) -> None:
        """Initialize the class.

        Args:
            *args: Variable length argument list
            logger: Optional logger object
            log_level: Log level (default: logging.INFO)
            log_fmt: Log format (default: "%(asctime)s %(levelname)s - %(message)s")
            **kwargs: Arbitrary keyword arguments

        Returns:
            None
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        self.logger = logger
        self.log_level = log_level
        self._fmt = log_fmt

    @property
    def fmt(self) -> str:  # pragma: no cover
        return self._fmt or ""

    def _get_log_context(
        self,
        message: Optional[StreamMessage[MsgType]],
        **kwargs: str,
    ) -> AnyDict:
        """Get the log context.

        Args:
            message: Optional stream message
            **kwargs: Additional keyword arguments

        Returns:
            A dictionary containing the log context with the following keys:
                - message_id: The first 10 characters of the message_id if message is not None, otherwise an empty string
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        return {
            "message_id": message.message_id[:10] if message else "",
        }

    def _log(
        self,
        message: str,
        log_level: Optional[int] = None,
        extra: Optional[AnyDict] = None,
        exc_info: Optional[Exception] = None,
    ) -> None:
        """Logs a message.

        Args:
            message: The message to be logged.
            log_level: The log level of the message. If not provided, the default log level of the logger will be used.
            extra: Additional information to be logged along with the message. This should be a dictionary.
            exc_info: An exception to be logged along with the message.

        Returns:
            None
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        if self.logger is not None:
            self.logger.log(
                level=(log_level or self.log_level),
                msg=message,
                extra=extra,
                exc_info=exc_info,
            )
