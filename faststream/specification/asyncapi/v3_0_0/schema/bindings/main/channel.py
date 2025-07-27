from pydantic import BaseModel
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream.specification.asyncapi.v3_0_0.schema.bindings import (
    amqp as amqp_bindings,
    kafka as kafka_bindings,
    nats as nats_bindings,
    redis as redis_bindings,
    sqs as sqs_bindings,
)
from faststream.specification.schema.bindings import ChannelBinding as SpecBinding


class ChannelBinding(BaseModel):
    """A class to represent channel bindings.

    Attributes:
        amqp : AMQP channel binding (optional)
        kafka : Kafka channel binding (optional)
        sqs : SQS channel binding (optional)
        nats : NATS channel binding (optional)
        redis : Redis channel binding (optional)
    """

    amqp: amqp_bindings.ChannelBinding | None = None
    kafka: kafka_bindings.ChannelBinding | None = None
    sqs: sqs_bindings.ChannelBinding | None = None
    nats: nats_bindings.ChannelBinding | None = None
    redis: redis_bindings.ChannelBinding | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"

    @classmethod
    def from_sub(cls, binding: SpecBinding | None) -> Self | None:
        if binding is None:
            return None

        if binding.amqp and (amqp := amqp_bindings.ChannelBinding.from_sub(binding.amqp)):
            return cls(amqp=amqp)

        if binding.kafka and (
            kafka := kafka_bindings.ChannelBinding.from_sub(binding.kafka)
        ):
            return cls(kafka=kafka)

        if binding.nats and (nats := nats_bindings.ChannelBinding.from_sub(binding.nats)):
            return cls(nats=nats)

        if binding.redis and (
            redis := redis_bindings.ChannelBinding.from_sub(binding.redis)
        ):
            return cls(redis=redis)

        if binding.sqs and (sqs := sqs_bindings.ChannelBinding.from_sub(binding.sqs)):
            return cls(sqs=sqs)

        return None

    @classmethod
    def from_pub(cls, binding: SpecBinding | None) -> Self | None:
        if binding is None:
            return None

        if binding.amqp and (amqp := amqp_bindings.ChannelBinding.from_pub(binding.amqp)):
            return cls(amqp=amqp)

        if binding.kafka and (
            kafka := kafka_bindings.ChannelBinding.from_pub(binding.kafka)
        ):
            return cls(kafka=kafka)

        if binding.nats and (nats := nats_bindings.ChannelBinding.from_pub(binding.nats)):
            return cls(nats=nats)

        if binding.redis and (
            redis := redis_bindings.ChannelBinding.from_pub(binding.redis)
        ):
            return cls(redis=redis)

        if binding.sqs and (sqs := sqs_bindings.ChannelBinding.from_pub(binding.sqs)):
            return cls(sqs=sqs)

        return None
