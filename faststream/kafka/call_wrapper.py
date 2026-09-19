from aiokafka import ConsumerRecord

from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
from faststream._internal.kafka import KafkaCallAssertions
from faststream._internal.testing.calls import field_reader
from faststream._internal.types import P_HandlerParams, T_HandlerReturn


class KafkaHandlerCallWrapper(
    KafkaCallAssertions,
    HandlerCallWrapper[P_HandlerParams, T_HandlerReturn],
):
    """The wrapper of a Kafka handler: its Call assertions take `key` and `partition`."""

    __slots__ = ()

    _read_field = field_reader("Kafka", ConsumerRecord, getattr)
