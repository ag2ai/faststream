from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.kafka.configs import KafkaBrokerConfig
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.specification.schema.bindings import ChannelBinding, kafka

from .config import KafkaPublisherSpecificationConfig


class KafkaPublisherSpecification(
    PublisherSpecification[KafkaBrokerConfig, KafkaPublisherSpecificationConfig],
):
    @property
    def topic(self) -> str:
        return f"{self._outer_config.prefix}{self.config.topic}"

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.topic}:Publisher"

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
                    kafka=kafka.ChannelBinding(
                        topic=self.topic,
                        partitions=None,
                        replicas=None,
                    ),
                ),
            ),
        }
