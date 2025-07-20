from faststream._internal.testing.app import TestApp

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
    from .testing import TestRabbitBroker

except ImportError as e:
    if "'aio_pika'" not in e.msg:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_RABBIT

    raise ImportError(INSTALL_FASTSTREAM_RABBIT) from e

__all__ = (
    "Channel",
    "ExchangeType",
    "QueueType",
    "RabbitBroker",
    "RabbitExchange",
    "RabbitMessage",
    "RabbitPublishCommand",
    "RabbitPublisher",
    "RabbitQueue",
    "RabbitResponse",
    "RabbitRoute",
    "RabbitRouter",
    "TestApp",
    "TestRabbitBroker",
)
