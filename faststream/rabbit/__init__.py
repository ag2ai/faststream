from importlib.util import find_spec
from typing import TYPE_CHECKING, TypeAlias

from faststream._internal.parser import ParserProto
from faststream._internal.testing.app import TestApp

if TYPE_CHECKING:
    from aio_pika import IncomingMessage

RabbitParserType: TypeAlias = ParserProto["IncomingMessage"]

try:
    from .annotations import RabbitMessage
    from .broker import RabbitBroker, RabbitPublisher, RabbitRoute, RabbitRouter
    from .response import RabbitPublishCommand, RabbitResponse
    from .schemas import (
        Channel,
        ExchangeType,
        QueueType,
        RabbitExchange,
        RabbitQueue,
    )
    from .security import RabbitExternalAuth
    from .testing import TestRabbitBroker

except ImportError as e:
    # the package is installed: the failure is its own, not a missing extra
    if find_spec("aio_pika") is not None:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_RABBIT

    raise ImportError(INSTALL_FASTSTREAM_RABBIT) from e

__all__ = (
    "Channel",
    "ExchangeType",
    "QueueType",
    "RabbitBroker",
    "RabbitExchange",
    "RabbitExternalAuth",
    "RabbitMessage",
    "RabbitParserType",
    "RabbitPublishCommand",
    "RabbitPublisher",
    "RabbitQueue",
    "RabbitResponse",
    "RabbitRoute",
    "RabbitRouter",
    "TestApp",
    "TestRabbitBroker",
)
