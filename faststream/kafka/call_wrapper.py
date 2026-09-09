from typing import TYPE_CHECKING, Any

from aiokafka import ConsumerRecord

from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
from faststream._internal.kafka.calls import KafkaCallAssertions
from faststream._internal.types import P_HandlerParams, T_HandlerReturn
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from faststream.message import StreamMessage


def read_field(name: str, message: "StreamMessage[Any]") -> Any:
    """Take a Kafka field off an aiokafka record, under the name `publish()` gives it."""
    raw = message.raw_message
    if not isinstance(raw, ConsumerRecord):
        msg = (
            f"`{name}` is a Kafka field, and this message did not come from Kafka: "
            f"its raw message is a `{type(raw).__name__}`."
        )
        raise SetupError(msg)
    return getattr(raw, name)


class KafkaHandlerCallWrapper(
    KafkaCallAssertions,
    HandlerCallWrapper[P_HandlerParams, T_HandlerReturn],
):
    """The wrapper of a Kafka handler: its Call assertions take `key` and `partition`."""

    __slots__ = ()

    _read_field = staticmethod(read_field)
