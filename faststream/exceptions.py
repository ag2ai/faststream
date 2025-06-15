from typing import Any, Iterable


class FastStreamException(Exception):  # noqa: N818
    """Basic FastStream exception class."""


class IgnoredException(FastStreamException):
    """Basic Exception class ignoring by watcher context and log middleware."""


class StopConsume(IgnoredException):
    """Raise it to stop Handler consuming."""

    def __str__(self) -> str:
        return "Consumer was stopped"


class StopApplication(IgnoredException, SystemExit):
    """Raise it to stop FastStream application."""

    def __str__(self) -> str:
        return "Application was stopped"


class HandlerException(IgnoredException):
    """Base Handler Exception."""


class SkipMessage(HandlerException):
    """Watcher Instruction to skip message."""

    def __str__(self) -> str:
        return "Message was skipped"


class AckMessage(HandlerException):
    """Exception raised to acknowledge a message immediately.

    This exception can be used to ack a message with additional options.
    To watch all allowed parameters, please take a look at your broker `message.ack(**extra_options)` method
    signature.

    Args:
        extra_options (Any): Additional parameters that will be passed to `message.ack(**extra_options)` method.
    """

    def __init__(self, **extra_options: Any):
        self.extra_options = extra_options
        super().__init__()

    def __str__(self) -> str:
        return "Message was acked"


class NackMessage(HandlerException):
    """Exception raised to negatively acknowledge a message immediately.

    This exception can be used to nack a message with additional options.
    To watch all allowed parameters, please take a look to your broker's `message.nack(**extra_options)` method
    signature.

    Args:
        kwargs (Any): Additional parameters that will be passed to `message.nack(**extra_options)` method.
    """

    def __init__(self, **kwargs: Any):
        self.extra_options = kwargs
        super().__init__()

    def __str__(self) -> str:
        return "Message was nacked"


class RejectMessage(HandlerException):
    """Exception raised to reject a message immediately.

    This exception can be used to reject a message with additional options.
    To watch all allowed parameters, please take a look to your broker's `message.reject(**extra_options)` method
    signature.

    Args:
        kwargs (Any): Additional parameters that will be passed to `message.reject(**extra_options)` method.
    """

    def __init__(self, **kwargs: Any):
        self.extra_options = kwargs
        super().__init__()

    def __str__(self) -> str:
        return "Message was rejected"


class SetupError(FastStreamException, ValueError):
    """Exception to raise at wrong method usage."""


class ValidationError(FastStreamException, ValueError):
    """Exception to raise at startup hook validation error."""

    def __init__(self, fields: Iterable[str] = ()) -> None:
        self.fields = fields


class OperationForbiddenError(FastStreamException, NotImplementedError):
    """Raises at planned NotImplemented operation call."""


class SubscriberNotFound(FastStreamException):
    """Raises as a service message or in tests."""


WRONG_PUBLISH_ARGS = SetupError(
    "You should use `reply_to` to send response to long-living queue "
    "and `rpc` to get response in sync mode."
)


NOT_CONNECTED_YET = "Please, `connect()` the broker first."


INSTALL_YAML = """
To use feature which need yaml, please install dependencies:\n
pip install PyYAML
"""

INSTALL_TOML = """
To use feature which need toml, please install dependencies:\n
pip install tomli
"""

INSTALL_WATCHFILES = """
To use restart feature, please install dependencies:\n
pip install watchfiles
"""


INSTALL_FASTSTREAM_RABBIT = """
To use RabbitMQ with FastStream, please install dependencies:\n
pip install 'faststream[rabbit]'
"""

INSTALL_FASTSTREAM_KAFKA = """
To use Apache Kafka with FastStream, please install dependencies:\n
pip install 'faststream[kafka]'
"""

INSTALL_FASTSTREAM_CONFLUENT = """
To use Confluent Kafka with FastStream, please install dependencies:\n
pip install 'faststream[confluent]'
"""


INSTALL_FASTSTREAM_REDIS = """
To use Redis with FastStream, please install dependencies:\n
pip install 'faststream[redis]'
"""

INSTALL_FASTSTREAM_NATS = """
To use NATS with FastStream, please install dependencies:\n
pip install 'faststream[nats]'
"""

INSTALL_UVICORN = """
To run FastStream ASGI App via CLI, please install uvicorn:\n
pip install uvicorn
"""
