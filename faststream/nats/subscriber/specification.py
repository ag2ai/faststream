from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream._internal.utils.path import Address
from faststream.nats.configs import NatsBrokerConfig
from faststream.nats.schemas.js_stream import NATS_ADDRESS_SYNTAX
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import ChannelBinding, nats

from .config import NatsSubscriberSpecificationConfig


class NatsSubscriberSpecification(
    SubscriberSpecification[NatsBrokerConfig, NatsSubscriberSpecificationConfig],
):
    @property
    def subject(self) -> "Address":
        """The subject this endpoint was declared with, and its Broker address."""
        return Address(self.config.subject, NATS_ADDRESS_SYNTAX).add_prefix(
            self._outer_config.prefix,
        )

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.subject.template}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        return {
            self.name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                    bindings=None,
                ),
                bindings=ChannelBinding(
                    nats=nats.ChannelBinding(
                        subject=self.subject.template,
                        queue=self.config.queue,
                    ),
                ),
            ),
        }


class NotIncludeSpecifation(SubscriberSpecification):
    @property
    def include_in_schema(self) -> bool:
        return False

    @property
    def name(self) -> str:
        raise NotImplementedError

    def get_schema(self) -> dict[str, "SubscriberSpec"]:
        raise NotImplementedError
