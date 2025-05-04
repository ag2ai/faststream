import logging
from collections import defaultdict
from enum import Enum
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from faststream._internal.application import Application


class LogLevels(str, Enum):
    """A class to represent log levels.

    Attributes:
        critical : critical log level
        error : error log level
        warning : warning log level
        info : info log level
        debug : debug log level
    """

    critical = "critical"
    fatal = "fatal"
    error = "error"
    warning = "warning"
    warn = "warn"
    info = "info"
    debug = "debug"
    notset = "notset"


LOG_LEVELS: defaultdict[str, int] = defaultdict(
    lambda: logging.INFO,
    critical=logging.CRITICAL,
    fatal=logging.FATAL,
    error=logging.ERROR,
    warning=logging.WARNING,
    warn=logging.WARNING,
    info=logging.INFO,
    debug=logging.DEBUG,
    notset=logging.NOTSET,
)


def get_log_level(level: Union[LogLevels, str, int]) -> int:
    """Get the log level.

    Args:
        level: The log level to get. Can be an integer, a LogLevels enum value, or a string.

    Returns:
        The log level as an integer.

    """
    if isinstance(level, int):
        return level

    if isinstance(level, LogLevels):
        return LOG_LEVELS[level.value]

    if isinstance(level, str):  # pragma: no branch
        return LOG_LEVELS[level.lower()]

    return None


def set_log_level(level: int, app: "Application") -> None:
    """Sets the log level for an application."""
    if app.logger and getattr(app.logger, "setLevel", None):
        app.logger.setLevel(level)  # type: ignore[attr-defined]

    for broker in app.brokers:
        broker._state.get().logger_state.set_level(level)
