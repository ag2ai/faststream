from faststream._internal.publisher.specified import (
    SpecificationPublisher as SpecificationPublisherMixin,
)
from faststream.confluent.publisher.usecase import BatchPublisher, DefaultPublisher
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.specification.schema.bindings import ChannelBinding, kafka


class SpecificationPublisher(SpecificationPublisherMixin):
    """A class representing a publisher."""

    def get_default_name(self) -> str:
        return f"{self.topic}:Publisher"

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
                    kafka=kafka.ChannelBinding(
                        topic=self.topic, partitions=None, replicas=None
                    )
                ),
            ),
        }


class SpecificationBatchPublisher(SpecificationPublisher, BatchPublisher):
    pass


class SpecificationDefaultPublisher(SpecificationPublisher, DefaultPublisher):
    pass
