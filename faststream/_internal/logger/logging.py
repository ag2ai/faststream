import logging
import sys
from collections.abc import Mapping
from logging import LogRecord
from typing import TYPE_CHECKING, TextIO

from .formatter import ColourizedFormatter

if TYPE_CHECKING:
    from faststream._internal.context.repository import ContextRepo


class ExtendedFilter(logging.Filter):
    def __init__(
        self,
        default_context: Mapping[str, str],
        message_id_ln: int,
        context: "ContextRepo",
        name: str = "",
    ) -> None:
        self.default_context = default_context
        self.message_id_ln = message_id_ln
        self.context = context
        super().__init__(name)

    def filter(self, record: LogRecord) -> bool:
        if is_suitable := super().filter(record):
            log_context: Mapping[str, str] = self.context.get_local(
                "log_context",
                self.default_context,
            )

            for k, v in log_context.items():
                value = getattr(record, k, v)
                setattr(record, k, value)

            record.message_id = getattr(record, "message_id", "")[: self.message_id_ln]

        return is_suitable


def get_broker_logger(
    name: str,
    default_context: Mapping[str, str],
    message_id_ln: int,
    fmt: str,
    context: "ContextRepo",
    log_level: int,
) -> logging.Logger:
    logger = get_logger(
        name=f"faststream.access.{name}",
        log_level=log_level,
        stream=sys.stdout,
        fmt=fmt,
    )
    logger.addFilter(ExtendedFilter(default_context, message_id_ln, context=context))
    return logger


def get_logger(name: str, log_level: int, stream: "TextIO", fmt: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.propagate = False
    set_logger_fmt(logger, stream=stream, fmt=fmt)
    return logger


def set_logger_fmt(
    logger: logging.Logger,
    stream: "TextIO",
    fmt: str,
) -> None:
    if _handler_exists(logger):
        return

    handler = logging.StreamHandler(stream=stream)
    handler.setFormatter(
        ColourizedFormatter(
            fmt=fmt,
            use_colors=True,
        ),
    )
    logger.addHandler(handler)


def _handler_exists(logger: logging.Logger) -> bool:
    # Check if a StreamHandler for sys.stdout already exists in the logger.
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
            return True
    return False


logger = get_logger(
    name="faststream",
    log_level=logging.INFO,
    stream=sys.stderr,
    fmt="%(asctime)s %(levelname)8s - %(message)s",
)
