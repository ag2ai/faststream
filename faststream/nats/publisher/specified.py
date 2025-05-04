from faststream._internal.publisher.specified import (
    SpecificationPublisher as SpecificationPublisherMixin,
)
from faststream.nats.publisher.usecase import LogicPublisher
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.specification.schema.bindings import ChannelBinding, nats


class SpecificationPublisher(
    SpecificationPublisherMixin,
    LogicPublisher,
):
    """A class to represent a NATS publisher."""

    def get_default_name(self) -> str:
        return f"{self.subject}:Publisher"

    def get_schema(self) -> dict[str, PublisherSpec]:
        payloads = self.get_payloads()

        return {
            self.name: PublisherSpec(
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
                    ),
                    bindings=None,
                ),
                bindings=ChannelBinding(
                    nats=nats.ChannelBinding(
                        subject=self.subject,
                        queue=None,
                    ),
                ),
            ),
        }
