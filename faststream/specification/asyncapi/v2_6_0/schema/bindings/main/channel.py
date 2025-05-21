from typing import Optional, overload

from pydantic import BaseModel
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream.specification.asyncapi.v2_6_0.schema.bindings import (
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

    amqp: Optional[amqp_bindings.ChannelBinding] = None
    kafka: Optional[kafka_bindings.ChannelBinding] = None
    sqs: Optional[sqs_bindings.ChannelBinding] = None
    nats: Optional[nats_bindings.ChannelBinding] = None
    redis: Optional[redis_bindings.ChannelBinding] = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"

    @overload
    @classmethod
    def from_sub(cls, binding: None) -> None: ...

    @overload
    @classmethod
    def from_sub(cls, binding: SpecBinding) -> Self: ...

    @classmethod
    def from_sub(cls, binding: Optional[SpecBinding]) -> Optional[Self]:
        if binding is None:
            return None

        if binding.amqp and (
            amqp := amqp_bindings.ChannelBinding.from_sub(binding.amqp)
        ):
            return cls(amqp=amqp)

        if binding.kafka and (
            kafka := kafka_bindings.ChannelBinding.from_sub(binding.kafka)
        ):
            return cls(kafka=kafka)

        if binding.nats and (
            nats := nats_bindings.ChannelBinding.from_sub(binding.nats)
        ):
            return cls(nats=nats)

        if binding.redis and (
            redis := redis_bindings.ChannelBinding.from_sub(binding.redis)
        ):
            return cls(redis=redis)

        if binding.sqs and (sqs := sqs_bindings.ChannelBinding.from_sub(binding.sqs)):
            return cls(sqs=sqs)

        return None

    @overload
    @classmethod
    def from_pub(cls, binding: None) -> None: ...

    @overload
    @classmethod
    def from_pub(cls, binding: SpecBinding) -> Self: ...

    @classmethod
    def from_pub(cls, binding: Optional[SpecBinding]) -> Optional[Self]:
        if binding is None:
            return None

        if binding.amqp and (
            amqp := amqp_bindings.ChannelBinding.from_pub(binding.amqp)
        ):
            return cls(amqp=amqp)

        if binding.kafka and (
            kafka := kafka_bindings.ChannelBinding.from_pub(binding.kafka)
        ):
            return cls(kafka=kafka)

        if binding.nats and (
            nats := nats_bindings.ChannelBinding.from_pub(binding.nats)
        ):
            return cls(nats=nats)

        if binding.redis and (
            redis := redis_bindings.ChannelBinding.from_pub(binding.redis)
        ):
            return cls(redis=redis)

        if binding.sqs and (sqs := sqs_bindings.ChannelBinding.from_pub(binding.sqs)):
            return cls(sqs=sqs)

        return None
