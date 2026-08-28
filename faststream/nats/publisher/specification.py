from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream._internal.utils.path import Address
from faststream.nats.configs import NatsBrokerConfig
from faststream.nats.schemas.js_stream import NATS_ADDRESS_SYNTAX
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.specification.schema.bindings import ChannelBinding, nats

from .config import NatsPublisherSpecificationConfig


class NatsPublisherSpecification(
    PublisherSpecification[NatsBrokerConfig, NatsPublisherSpecificationConfig],
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

        return f"{self.subject.template}:Publisher"

    def get_schema(self) -> dict[str, PublisherSpec]:
        payloads = self.get_payloads()

        return {
            self.name: PublisherSpec(
                description=self.config.description_,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
                    ),
                    bindings=None,
                ),
                bindings=ChannelBinding(
                    nats=nats.ChannelBinding(
                        subject=self.subject.template,
                        queue=None,
                    ),
                ),
            ),
        }
