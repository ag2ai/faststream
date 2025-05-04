from dataclasses import dataclass
from typing import Optional

from faststream.specification.schema.bindings import (
    amqp as amqp_bindings,
    kafka as kafka_bindings,
    nats as nats_bindings,
    redis as redis_bindings,
    sqs as sqs_bindings,
)


@dataclass
class ChannelBinding:
    """A class to represent channel bindings.

    Attributes:
        amqp : AMQP channel binding (optional)
        kafka : Kafka channel binding (optional)
        sqs : SQS channel binding (optional)
        nats : NATS channel binding (optional)d
        redis : Redis channel binding (optional)
    """

    amqp: Optional[amqp_bindings.ChannelBinding] = None
    kafka: Optional[kafka_bindings.ChannelBinding] = None
    sqs: Optional[sqs_bindings.ChannelBinding] = None
    nats: Optional[nats_bindings.ChannelBinding] = None
    redis: Optional[redis_bindings.ChannelBinding] = None


@dataclass
class OperationBinding:
    """A class to represent an operation binding.

    Attributes:
        amqp : AMQP operation binding (optional)
        kafka : Kafka operation binding (optional)
        sqs : SQS operation binding (optional)
        nats : NATS operation binding (optional)
        redis : Redis operation binding (optional)
    """

    amqp: Optional[amqp_bindings.OperationBinding] = None
    kafka: Optional[kafka_bindings.OperationBinding] = None
    sqs: Optional[sqs_bindings.OperationBinding] = None
    nats: Optional[nats_bindings.OperationBinding] = None
    redis: Optional[redis_bindings.OperationBinding] = None
