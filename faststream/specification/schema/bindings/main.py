from dataclasses import dataclass

from faststream.specification.schema.bindings import (
    amqp as amqp_bindings,
    http as http_bindings,
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

    amqp: amqp_bindings.ChannelBinding | None = None
    kafka: kafka_bindings.ChannelBinding | None = None
    sqs: sqs_bindings.ChannelBinding | None = None
    nats: nats_bindings.ChannelBinding | None = None
    redis: redis_bindings.ChannelBinding | None = None


@dataclass
class OperationBinding:
    """A class to represent an operation binding.

    Attributes:
        amqp : AMQP operation binding (optional)
        kafka : Kafka operation binding (optional)
        sqs : SQS operation binding (optional)
        nats : NATS operation binding (optional)
        redis : Redis operation binding (optional)
        http: HTTP operation binding (optional)
    """

    amqp: amqp_bindings.OperationBinding | None = None
    kafka: kafka_bindings.OperationBinding | None = None
    sqs: sqs_bindings.OperationBinding | None = None
    nats: nats_bindings.OperationBinding | None = None
    redis: redis_bindings.OperationBinding | None = None
    http: http_bindings.OperationBinding | None = None
