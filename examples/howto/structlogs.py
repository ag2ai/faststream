import logging
import sys

import structlog

from faststream import FastStream, context
from faststream.kafka import KafkaBroker


def merge_contextvars(
    logger: structlog.types.WrappedLogger,
    method_name: str,
    event_dict: structlog.types.EventDict,
) -> structlog.types.EventDict:
    event_dict["extra"] = event_dict.get(
        "extra",
        context.get_local("log_context") or {},
    )
    return event_dict


shared_processors = (
    merge_contextvars,
    structlog.processors.add_log_level,
    structlog.processors.StackInfoRenderer(),
    structlog.dev.set_exc_info,
    structlog.processors.TimeStamper(fmt="iso"),
)

if sys.stderr.isatty():
    processors = [
        *shared_processors,
        structlog.dev.ConsoleRenderer(),
    ]
else:
    processors = [
        *shared_processors,
        structlog.processors.dict_tracebacks,
        structlog.processors.JSONRenderer(),
    ]

structlog.configure(
    processors=processors,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False,
)
logger = structlog.get_logger()


broker = KafkaBroker(logger=logger, log_level=logging.DEBUG)
app = FastStream(broker, logger=logger)
